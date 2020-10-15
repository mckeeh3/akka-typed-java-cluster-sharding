package cluster;

import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.cluster.ClusterEvent;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import akka.cluster.typed.Cluster;
import akka.http.javadsl.Http;
import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.MediaTypes;
import akka.http.javadsl.model.headers.RawHeader;
import akka.http.javadsl.model.ws.Message;
import akka.http.javadsl.model.ws.TextMessage;
import akka.http.javadsl.server.Route;
import akka.japi.JavaPartialFunction;
import akka.stream.javadsl.Flow;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.slf4j.Logger;
import scala.Option;

import java.io.Serializable;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static akka.http.javadsl.server.Directives.*;

class HttpServer {
  private final ActorSystem<?> actorSystem;
  private ClusterAwareStatistics clusterAwareStatistics;
  private SingletonAwareStatistics singletonAwareStatistics;
  private final Tree tree = new Tree("cluster", "cluster");

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
    log().info("HTTP Server started on port {}", "" + port);
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
        path("favicon.ico", () -> getFromResource("favicon.ico", MediaTypes.IMAGE_X_ICON.toContentType())));
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
    String messageText = message.asTextMessage().getStrictText();
    if (messageText.startsWith("akka.tcp")) {
      broadcastStopNode(messageText);
    }
    return getTreeAsJson();
  }

  private void broadcastStopNode(String memberAddress) {
    // cluster.state().getMembers().forEach(member -> forwardAction(new StopNode(memberAddress), member));
  }

  private Message getTreeAsJson() {
    tree.setMemberType(Cluster.get(actorSystem).selfMember().address().toString(), "httpServer");
    return TextMessage.create(tree.toJson());
  }

  private static Nodes loadNodes(ActorSystem<?> actorSystem, ClusterAwareStatistics clusterAwareStatistics, SingletonAwareStatistics singletonAwareStatistics) {
    final Cluster cluster = Cluster.get(actorSystem);
    final ClusterEvent.CurrentClusterState clusterState = cluster.state();

    final Set<Member> unreachable = clusterState.getUnreachable();

    final Optional<Member> old = StreamSupport.stream(clusterState.getMembers().spliterator(), false)
        .filter(member -> member.status().equals(MemberStatus.up()))
        .filter(member -> !(unreachable.contains(member)))
        .reduce((older, member) -> older.isOlderThan(member) ? older : member);

    final Member oldest = old.orElse(cluster.selfMember());

    final List<Integer> seedNodePorts = seedNodePorts(actorSystem);

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
    final Option<Object> portOption = member.address().port();
    return portOption.isDefined() ? Integer.parseInt(portOption.get().toString()) : 0;
  }

  private static List<Integer> seedNodePorts(ActorSystem<?> actorSystem) {
    return actorSystem.settings().config().getList("akka.cluster.seed-nodes").stream().map(s -> s.unwrapped().toString()).map(s -> {
          final String[] split = s.split(":");
          return split.length == 0 ? 0 : Integer.parseInt(split[split.length - 1]);
        }).collect(Collectors.toList());
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

  void load(EntityAction action) {
    if ("start".equals(action.action)) {
      tree.add(action.member, action.shardId, action.entityId);
    } else if ("stop".equals(action.action)) {
      tree.remove(action.member, action.shardId, action.entityId);
    }
  }

  public static class EntityAction implements Statistics {
    final String member;
    final String shardId;
    final String entityId;
    final String action;

    @JsonCreator
    EntityAction(String member, String shardId, String entityId, String action) {
      this.member = member;
      this.shardId = shardId;
      this.entityId = entityId;
      this.action = action;
    }

    @Override
    public String toString() {
      return String.format("%s[%s, %s, %s, %s]", getClass().getSimpleName(), member, shardId, entityId, action);
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
      final int port = memberPort(member);
      if (isValidPort(port)) {
        nodes.add(new Node(port, state(member.status()), memberStatus(member.status()), leader, oldest, seedNode));
      }
    }

    void addUnreachable(Member member) {
      final int port = memberPort(member);
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
      final ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
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
      removeEntity(entityId);
      Tree member = find(memberId, "member");
      if (member == null) {
        member = Tree.create(memberId, "member");
        children.add(member);
      }
      Tree shard = member.find(shardId, "shard");
      if (shard == null) {
        shard = Tree.create(shardId, "shard");
        member.children.add(shard);
      }
      Tree entity = shard.find(entityId, "entity");
      if (entity == null) {
        entity = Tree.create(entityId, "entity");
        shard.children.add(entity);
      }
    }

    void remove(String memberId, String shardId, String entityId) {
      Tree member = find(memberId, "member");
      if (member != null) {
        Tree shard = member.find(shardId, "shard");
        if (shard != null) {
          Tree entity = shard.find(entityId, "entity");
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
      for (Tree member : children) {
        for (Tree shard : member.children) {
          for (Tree entity : shard.children) {
            if (entity.name.equals(entityId)) {
              shard.children.remove(entity);
              break;
            }
          }
        }
      }
    }

    void incrementEvents(String memberId, String shardId, String entityId) {
      Tree entity = find(memberId, shardId, entityId);
      if (entity != null) {
        entity.events += 1;
      }
    }

    private Tree find(String memberId, String shardId, String entityId) {
      Tree member = find(memberId, "member");
      if (member != null) {
        Tree shard = member.find(shardId, "shard");
        if (shard != null) {
          Tree entity = shard.find(entityId, "entity");
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
        for (Tree child : children) {
          Tree found = child.find(name, type);
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
      Tree member = find(memberId, type);
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
      ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
      try {
        return ow.writeValueAsString(this);
      } catch (JsonProcessingException e) {
        return String.format("{ \"error\" : \"%s\" }", e.getMessage());
      }
    }

    @Override
    public String toString() {
      return String.format(
        "%s[%s, %s, %d]",
        getClass().getSimpleName(),
        name,
        type,
        events
      );
    }
  }
}