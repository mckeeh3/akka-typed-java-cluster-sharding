package cluster;

import static akka.http.javadsl.server.Directives.complete;
import static akka.http.javadsl.server.Directives.concat;
import static akka.http.javadsl.server.Directives.get;
import static akka.http.javadsl.server.Directives.getFromResource;
import static akka.http.javadsl.server.Directives.handleWebSocketMessages;
import static akka.http.javadsl.server.Directives.path;
import static akka.http.javadsl.server.Directives.respondWithHeader;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import org.slf4j.Logger;

import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import akka.cluster.typed.Cluster;
import akka.cluster.typed.Leave;
import akka.http.javadsl.Http;
import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.MediaTypes;
import akka.http.javadsl.model.headers.RawHeader;
import akka.http.javadsl.model.ws.Message;
import akka.http.javadsl.model.ws.TextMessage;
import akka.http.javadsl.server.Route;
import akka.japi.JavaPartialFunction;
import akka.stream.javadsl.Flow;
import cluster.HttpServer.ServerActivitySummary.ServerActivity;

class HttpServer {
  private final ActorSystem<?> actorSystem;
  private ClusterAwareStatistics clusterAwareStatistics;
  private SingletonAwareStatistics singletonAwareStatistics;
  private final Tree tree = new Tree("cluster", "cluster");
  private final ActivitySummary activitySummary = new ActivitySummary();

  static HttpServer start(ActorSystem<?> actorSystem) {
    final int port = memberPort(Cluster.get(actorSystem).selfMember());
    if (port >= 2551 && port <= 2559) {
      return new HttpServer(port + 7000, actorSystem);
    } else {
      final String message = String
          .format("HTTP server not started. Node port %d is invalid. The port must be >= 2551 and <= 2559.", port);
      System.err.printf("%s%n", message);
      throw new RuntimeException(message);
    }
  }

  private HttpServer(int port, ActorSystem<?> actorSystem) {
    this.actorSystem = actorSystem;
    start(port);
  }

  private void start(int port) {
    Http.get(actorSystem).newServerAt("localhost", port).bind(route());
    log().info("HTTP Server started on port {}", port);
  }

  private Route route() {
    return concat(
        path("", () -> getFromResource("dashboard.html", ContentTypes.TEXT_HTML_UTF8)),
        path("dashboard", () -> getFromResource("dashboard.html", ContentTypes.TEXT_HTML_UTF8)),
        path("dashboard.html", () -> getFromResource("dashboard.html", ContentTypes.TEXT_HTML_UTF8)),
        path("dashboard.js", () -> getFromResource("dashboard.js", ContentTypes.APPLICATION_JSON)),
        path("dashboard-cluster-aware.js", () -> getFromResource("dashboard-cluster-aware.js", ContentTypes.APPLICATION_JSON)),
        path("dashboard-singleton-aware.js", () -> getFromResource("dashboard-singleton-aware.js", ContentTypes.APPLICATION_JSON)),
        path("p5.js", () -> getFromResource("p5.js", ContentTypes.APPLICATION_JSON)),
        path("cluster-state", this::clusterState),
        path("viewer", () -> getFromResource("viewer.html", ContentTypes.TEXT_HTML_UTF8)),
        path("viewer.html", () -> getFromResource("viewer.html", ContentTypes.TEXT_HTML_UTF8)),
        path("viewer.js", () -> getFromResource("viewer.js", ContentTypes.APPLICATION_JSON)),
        path("d3.v5.js", () -> getFromResource("d3.v5.js", MediaTypes.APPLICATION_JAVASCRIPT.toContentTypeWithMissingCharset())),
        path("viewer-entities", () -> handleWebSocketMessages(handleClientMessages())),
        path("favicon.ico", () -> getFromResource("favicon.ico", MediaTypes.IMAGE_X_ICON.toContentType()))
    );
  }

  private Route clusterState() {
    return get(() -> respondWithHeader(RawHeader.create("Access-Control-Allow-Origin", "*"),
        () -> complete(loadNodes(actorSystem, clusterAwareStatistics, singletonAwareStatistics).toJson())));
  }

  private Flow<Message, Message, NotUsed> handleClientMessages() {
    return Flow.<Message>create().collect(new JavaPartialFunction<Message, Message>() {
      @Override
      public Message apply(Message message, boolean isCheck) {
        if (isCheck && message.isText()) {
          return null;
        } else if (isCheck && !message.isText()) {
          throw noMatch();
        } else if (message.asTextMessage().isStrict()) {
          return handleClientMessage(message);
        } else {
          return TextMessage.create("");
        }
      }
    });
  }

  private Message handleClientMessage(Message message) {
    final var messageText = message.asTextMessage().getStrictText();
    if (messageText.startsWith("akka://")) {
      handleStopNode(messageText);
    }
    removeOfflineMembers(actorSystem, tree);

    return responseAsJson();
  }

  private void handleStopNode(String memberAddress) {
    log().info("Stop node {}", memberAddress);
    final var cluster = Cluster.get(actorSystem);
    cluster.state().getMembers().forEach(member -> {
      if (memberAddress.equals(member.address().toString())) {
        cluster.manager().tell(Leave.create(member.address()));
      }
    });
  }

  private Message responseAsJson() {
    tree.setMemberType(Cluster.get(actorSystem).selfMember().address().toString(), "httpServer");
    final var clientResponse = new ClientResponse(tree, activitySummary);
    return TextMessage.create(clientResponse.toJson());
  }

  private static Nodes loadNodes(ActorSystem<?> actorSystem, ClusterAwareStatistics clusterAwareStatistics, SingletonAwareStatistics singletonAwareStatistics) {
    final var cluster = Cluster.get(actorSystem);
    final var clusterState = cluster.state();
    final var unreachable = clusterState.getUnreachable();

    final var old = StreamSupport.stream(clusterState.getMembers().spliterator(), false)
        .filter(member -> member.status().equals(MemberStatus.up()))
        .filter(member -> !(unreachable.contains(member)))
        .reduce((older, member) -> older.isOlderThan(member) ? older : member);

    final var oldest = old.orElse(cluster.selfMember());

    final var seedNodePorts = seedNodePorts(actorSystem);

    final Nodes nodes = new Nodes(
        memberPort(cluster.selfMember()),
        cluster.selfMember().address().equals(clusterState.getLeader()),
        oldest.equals(cluster.selfMember()),
        clusterAwareStatistics, singletonAwareStatistics);

    StreamSupport.stream(clusterState.getMembers().spliterator(), false).forEach(new Consumer<Member>() {
      @Override
      public void accept(Member member) {
        nodes.add(member, leader(member), oldest(member), seedNode(member));
      }

      private boolean leader(Member member) {
        return member.address().equals(clusterState.getLeader());
      }

      private boolean oldest(Member member) {
        return oldest.equals(member);
      }

      private boolean seedNode(Member member) {
        return seedNodePorts.contains(memberPort(member));
      }
    });

    clusterState.getUnreachable().forEach(nodes::addUnreachable);

    return nodes;
  }

  private Logger log() {
    return actorSystem.log();
  }

  private static boolean isValidPort(int port) {
    return port >= 2551 && port <= 2559;
  }

  private static int memberPort(Member member) {
    final var portOption = member.address().port();
    return portOption.isDefined() ? Integer.parseInt(portOption.get().toString()) : 0;
  }

  private static List<Integer> seedNodePorts(ActorSystem<?> actorSystem) {
    return actorSystem.settings().config().getList("akka.cluster.seed-nodes").stream().map(s -> s.unwrapped().toString()).map(s -> {
          final var split = s.split(":");
          return split.length == 0 ? 0 : Integer.parseInt(split[split.length - 1]);
        }).collect(Collectors.toList());
  }

  private static void removeOfflineMembers(ActorSystem<?> actorSystem, Tree tree) {
    var liveMembers = liveMembers(actorSystem);

    tree.children.stream()
      .filter((c -> !liveMembers.contains(c.name)))
      .collect(Collectors.toList())
      .forEach(c -> {
        actorSystem.log().info("Removing offline member: {}", c.name);
        tree.removeMember(c.name);
      });
  }

  private static Set<String> liveMembers(ActorSystem<?> actorSystem) {
    final var cluster = Cluster.get(actorSystem);
    final var clusterState = cluster.state();
    final var unreachable = clusterState.getUnreachable();

    return StreamSupport.stream(clusterState.getMembers().spliterator(), false)
      .filter(member -> !(unreachable.contains(member)))
      .map(member -> member.address().toString())
      .collect(Collectors.toSet());
  }

  public interface Statistics extends CborSerializable {}

  void load(ClusterAwareStatistics clusterAwareStatistics) {
    this.clusterAwareStatistics = clusterAwareStatistics;
  }

  public static class ClusterAwareStatistics implements Statistics {
    public final int totalPings;
    public final int pingRatePs;
    public final Map<Integer, Integer> nodePings;

    public ClusterAwareStatistics(int totalPings, int pingRatePs, Map<Integer, Integer> nodePings) {
      this.totalPings = totalPings;
      this.pingRatePs = pingRatePs;
      this.nodePings = nodePings;
    }
  }

  void load(SingletonAwareStatistics singletonAwareStatistics) {
    this.singletonAwareStatistics = singletonAwareStatistics;
    tree.setMemberType(singletonAwareStatistics.memberId, "singleton");
  }

  public static class SingletonAwareStatistics implements Statistics {
    public final String memberId;
    public final int totalPings;
    public final int pingRatePs;
    public final Map<Integer, Integer> nodePings;

    public SingletonAwareStatistics(String memberId, int totalPings, int pingRatePs, Map<Integer, Integer> nodePings) {
      this.memberId = memberId;
      this.totalPings = totalPings;
      this.pingRatePs = pingRatePs;
      this.nodePings = nodePings;
    }

    @Override
    public String toString() {
      return String.format("%s[%s, %d, %d, %s]", getClass().getSimpleName(), memberId, totalPings, pingRatePs, nodePings);
    }
  }

  void load(EntityAction entityAction) {
    try {
      switch (entityAction.action) {
        case "start":
          tree.add(entityAction.member, entityAction.shardId, entityAction.entityId);
          activitySummary.load(entityAction);
          break;
        case "ping":
          tree.ping(entityAction.member, entityAction.shardId, entityAction.entityId);
          activitySummary.load(entityAction);
          break;
        case "stop":
          tree.remove(entityAction.member, entityAction.shardId, entityAction.entityId);
          break;
        default:
          break;
      }
    } catch (RuntimeException e) {
      log().error("Failed to load entity action: {}", e);
      log().error("Failed to load entity action: {}", entityAction);
      log().warn("Failed to load entity action: tree children ({}) {}", tree.children.size(), tree.toJson());
    }
  }

  public static class EntityAction implements Statistics {
    final String member;
    final String shardId;
    final String entityId;
    final String action;
    final String httpServer;

    @JsonCreator
    EntityAction(String member, String shardId, String entityId, String action, String httpServer) {
      this.member = member;
      this.shardId = shardId;
      this.entityId = entityId;
      this.action = action;
      this.httpServer = httpServer;
    }

    @Override
    public String toString() {
      return String.format("%s[%s, %s, %s, %s, %s]", getClass().getSimpleName(), member, shardId, entityId, action, httpServer);
    }
  }

  public static class Nodes implements CborSerializable {
    public final int selfPort;
    public final boolean leader;
    public final boolean oldest;
    public final ClusterAwareStatistics clusterAwareStatistics;
    public final SingletonAwareStatistics singletonAwareStatistics;
    public List<Node> nodes = new ArrayList<>();

    public Nodes(int selfPort, boolean leader, boolean oldest, ClusterAwareStatistics clusterAwareStatistics, SingletonAwareStatistics singletonAwareStatistics) {
      this.selfPort = selfPort;
      this.leader = leader;
      this.oldest = oldest;
      this.clusterAwareStatistics = clusterAwareStatistics;
      this.singletonAwareStatistics = singletonAwareStatistics;
    }

    void add(Member member, boolean leader, boolean oldest, boolean seedNode) {
      final var port = memberPort(member);
      if (isValidPort(port)) {
        nodes.add(new Node(port, state(member.status()), memberStatus(member.status()), leader, oldest, seedNode));
      }
    }

    void addUnreachable(Member member) {
      final var port = memberPort(member);
      if (isValidPort(port)) {
        Node node = new Node(port, "unreachable", "unreachable", false, false, false);
        nodes.remove(node);
        nodes.add(node);
      }
    }

    private static String state(MemberStatus memberStatus) {
      if (memberStatus.equals(MemberStatus.down())) {
        return "down";
      } else if (memberStatus.equals(MemberStatus.joining())) {
        return "starting";
      } else if (memberStatus.equals(MemberStatus.weaklyUp())) {
        return "starting";
      } else if (memberStatus.equals(MemberStatus.up())) {
        return "up";
      } else if (memberStatus.equals(MemberStatus.exiting())) {
        return "stopping";
      } else if (memberStatus.equals(MemberStatus.leaving())) {
        return "stopping";
      } else if (memberStatus.equals(MemberStatus.removed())) {
        return "stopping";
      } else {
        return "offline";
      }
    }

    private static String memberStatus(MemberStatus memberStatus) {
      if (memberStatus.equals(MemberStatus.down())) {
        return "down";
      } else if (memberStatus.equals(MemberStatus.joining())) {
        return "joining";
      } else if (memberStatus.equals(MemberStatus.weaklyUp())) {
        return "weaklyup";
      } else if (memberStatus.equals(MemberStatus.up())) {
        return "up";
      } else if (memberStatus.equals(MemberStatus.exiting())) {
        return "exiting";
      } else if (memberStatus.equals(MemberStatus.leaving())) {
        return "leaving";
      } else if (memberStatus.equals(MemberStatus.removed())) {
        return "removed";
      } else {
        return "unknown";
      }
    }

    String toJson() {
      final var ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
      try {
        return ow.writeValueAsString(this);
      } catch (JsonProcessingException e) {
        return String.format("{ \"error\" : \"%s\" }", e.getMessage());
      }
    }
  }

  public static class Node implements CborSerializable {
    public final int port;
    public final String state;
    public final String memberState;
    public final boolean leader;
    public final boolean oldest;
    public final boolean seedNode;

    public Node(int port, String state, String memberState, boolean leader, boolean oldest, boolean seedNode) {
      this.port = port;
      this.state = state;
      this.memberState = memberState;
      this.leader = leader;
      this.oldest = oldest;
      this.seedNode = seedNode;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Node node = (Node) o;
      return Objects.equals(port, node.port);
    }

    @Override
    public int hashCode() {
      return Objects.hash(port);
    }
  }

  public static class Tree implements Serializable {
    private static final long serialVersionUID = 1L;
    public final String name;
    public String type;
    public int events;
    public final List<Tree> children = new ArrayList<>();

    public Tree(String name, String type) {
      this.name = name;
      this.type = type;
    }

    static Tree create(String name, String type) {
      return new Tree(name, type);
    }

    Tree children(Tree... children) {
      this.children.addAll(Arrays.asList(children));
      return this;
    }

    void add(String memberId, String shardId, String entityId) {
      if (memberId == null) {
        throw new IllegalArgumentException("memberId must not be null");
      }
      if (shardId == null) {
        throw new IllegalArgumentException("shardId must not be null");
      }
      if (entityId == null) {
        throw new IllegalArgumentException("entityId must not be null");
      }
      removeEntity(entityId);
      var member = find(memberId, "member");
      if (member == null) {
        member = Tree.create(memberId, "member");
        children.add(member);
      }
      var shard = member.find(shardId, "shard");
      if (shard == null) {
        shard = Tree.create(shardId, "shard");
        member.children.add(shard);
      }
      var entity = shard.find(entityId, "entity");
      if (entity == null) {
        entity = Tree.create(entityId, "entity");
        shard.children.add(entity);
      }
    }

    void ping(String memberId, String shardId, String entityId) {
      final var entity = find(entityId, "entity");
      if (entity == null) {
        add(memberId, shardId, entityId);
      }
    }

    void remove(String memberId, String shardId, String entityId) {
      var member = find(memberId, "member");
      if (member != null) {
        var shard = member.find(shardId, "shard");
        if (shard != null) {
          var entity = shard.find(entityId, "entity");
          shard.children.remove(entity);

          if (shard.children.isEmpty()) {
            member.children.remove(shard);
          }
        }
        if (member.children.isEmpty()) {
          children.remove(member);
        }
      }
    }

    void removeEntity(String entityId) {
      var memberId = "";
      var shardId = "";
      for (var member : children) {
        for (var shard : member.children) {
          for (var entity : shard.children) {
            if (entity.name.equals(entityId)) {
              memberId = member.name;
              shardId = shard.name;
              break;
            }
          }
        }
      }
      if (!memberId.isEmpty()) {
        remove(memberId, shardId, entityId);
      }
    }

    private static class RemoveEntity {
      final String memberId;
      final String shardId;
      final String entityId;

      RemoveEntity(String memberId, String shardId, String entityId) {
        this.memberId = memberId;
        this.shardId = shardId;
        this.entityId = entityId;
      }
    }

    void removeMember(String memberId) {
      final var entities = new ArrayList<RemoveEntity>();
      for (var member : children) {
        if (member.name.equals(memberId)) {
          for (var shard : member.children) {
            for (var entity : shard.children) {
              entities.add(new RemoveEntity(member.name, shard.name, entity.name));
            }
          }
        }
      }
      entities.forEach(e -> remove(e.memberId, e.shardId, e.entityId));
    }

    void incrementEvents(String memberId, String shardId, String entityId) {
      final var entity = find(memberId, shardId, entityId);
      if (entity != null) {
        entity.events += 1;
      }
    }

    private Tree find(String memberId, String shardId, String entityId) {
      final var member = find(memberId, "member");
      if (member != null) {
        final var shard = member.find(shardId, "shard");
        if (shard != null) {
          final var entity = shard.find(entityId, "entity");
          if (entity != null) {
            return entity;
          }
        }
      }
      return null;
    }

    Tree find(String name, String type) {
      if (this.name.equals(name) && this.type.contains(type)) {
        return this;
      } else {
        for (var child : children) {
          final var found = child.find(name, type);
          if (found != null) {
            return found;
          }
        }
      }
      return null;
    }

    void setMemberType(String memberId, String type) {
      children.forEach(
        child -> {
          if (child.name.equals(memberId)) {
            if (!child.type.contains(type)) {
              child.type = child.type + " " + type;
            }
          } else if (child.type.contains(type)) {
            unsetMemberType(child.name, type);
          }
        }
      );
    }

    void unsetMemberType(String memberId, String type) {
      final var member = find(memberId, type);
      if (member != null) {
        member.type = member.type.replaceAll(type, "");
        member.type = member.type.replaceAll(" +", " ");
      }
    }

    int leafCount() {
      if (children.size() > 0) {
        return children.stream().mapToInt(Tree::leafCount).sum();
      } else {
        return 1;
      }
    }

    int eventsCount() {
      if (children.size() > 0) {
        return children.stream().mapToInt(Tree::eventsCount).sum();
      } else {
        return events;
      }
    }

    String toJson() {
      final var ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
      try {
        return ow.writeValueAsString(this);
      } catch (JsonProcessingException e) {
        return String.format("{ \"error\" : \"%s\" }", e.getMessage());
      }
    }

    @Override
    public String toString() {
      return String.format("%s[%s, %s, %d]", getClass().getSimpleName(), name, type, events);
    }
  }

  public static class ActivitySummary implements Serializable {
    private static final long serialVersionUID = 1L;
    public final ServerActivitySummary serverActivitySummary = new ServerActivitySummary();

    void load(EntityAction entityAction) {
      serverActivitySummary.load(entityAction);
    }

    @Override
    public String toString() {
      return String.format("%s[%s]", getClass().getSimpleName(), serverActivitySummary.serverActivities.values());
    }
  }

  public static class ServerActivitySummary implements Serializable {
    private static final long serialVersionUID = 1L;
    public final Map<String, ServerActivity> serverActivities = new HashMap<>();

    void load(EntityAction entityAction) {
      final String server = entityAction.httpServer;
      serverActivities.put(server, serverActivities.getOrDefault(server, new ServerActivity(server)).load(entityAction));
    }

    @Override
    public String toString() {
      return String.format("%s[%s]", getClass().getSimpleName(), serverActivities);
    }

    public static class ServerActivity implements Serializable {
      private static final long serialVersionUID = 1L;
      public final String server;
      public int messageCount;
      public Queue<Link> links = new LinkedList<>();

      public ServerActivity(String server) {
        this.server = server;
        messageCount = 0;
      }

      ServerActivity load(EntityAction entityAction) {
        messageCount++;

        links.offer(new Link(entityAction.entityId, entityAction.httpServer));
        while (links.size() > 50) {
          links.poll();
        }
        return this;
      }

      @Override
      public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((server == null) ? 0 : server.hashCode());
        return result;
      }

      @Override
      public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        ServerActivity other = (ServerActivity) obj;
        if (server == null) {
          if (other.server != null) return false;
        } else if (!server.equals(other.server)) return false;
        return true;
      }

      @Override
      public String toString() {
        return String.format("%s[%s, %,d]", getClass().getSimpleName(), server, messageCount);
      }
    }
  }

  public static class Link implements Serializable {
    private static final long serialVersionUID = 1L;
    public final String entityId;
    public final String server;

    public Link(String entityId, String server) {
      this.entityId = entityId;
      this.server = server;
    }

    @Override
    public String toString() {
      return String.format("%s[%s, %s]", getClass().getSimpleName(), entityId, server);
    }
  }

  public static class ClientResponse implements Serializable {
    private static final long serialVersionUID = 1L;
    public final Tree tree;
    public final Collection<ServerActivity> serverActivities;

    public ClientResponse(Tree tree, ActivitySummary activitySummary) {
      this.tree = tree;
      serverActivities = activitySummary.serverActivitySummary.serverActivities.values();
    }

    String toJson() {
      ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
      try {
        return ow.writeValueAsString(this);
      } catch (JsonProcessingException e) {
        return String.format("{ \"error\" : \"%s\" }", e.getMessage());
      }
    }
  }
}
