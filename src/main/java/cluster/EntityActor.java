package cluster;

import com.fasterxml.jackson.annotation.JsonCreator;

import org.slf4j.Logger;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import cluster.HttpServer.EntityAction;
import cluster.HttpServerActor.BroadcastEntityAction;

public class EntityActor extends AbstractBehavior<EntityActor.Command> {
  private final ActorContext<Command> actorContext;
  private final String entityId;
  private final String shardId;
  private final String memberId;
  private final ActorRef<HttpServer.Statistics> httpServerActorRef;
  private State state;
  static EntityTypeKey<Command> entityTypeKey = EntityTypeKey.create(Command.class, EntityActor.class.getSimpleName());

  static Behavior<Command> create(String entityId, ActorRef<HttpServer.Statistics> httpServerActorRef) {
    return Behaviors.setup(actorContext -> new EntityActor(actorContext, entityId, httpServerActorRef));
  }

  private EntityActor(ActorContext<Command> actorContext, String entityId, ActorRef<HttpServer.Statistics> httpServerActorRef) {
    super(actorContext);
    this.actorContext = actorContext;
    this.entityId = entityId;
    this.httpServerActorRef = httpServerActorRef;
    shardId = "" + Math.abs(entityId.hashCode()) % actorContext.getSystem().settings().config().getInt("akka.cluster.sharding.number-of-shards");
    memberId = actorContext.getSystem().address().toString();
    log().info("Start {}", entityId);
  }

  @Override
  public Receive<Command> createReceive() {
    return newReceiveBuilder()
      .onMessage(ChangeValue.class, this::onChangeValue)
      .onMessage(GetValue.class, this::onGetValue)
      .onMessage(Passivate.class, msg -> onPassivate())
      .build();
  }
  
  private Behavior<Command> onChangeValue(ChangeValue changeValue) {
    if (state == null) {
      state = new State(changeValue.id, changeValue.value);
      log().info("initialize {}", state);

      changeValue.replyTo.tell(new ChangeValueAck("initialize", changeValue.id, changeValue.value));
      notifyHttpServer("start", changeValue.replyTo);
    } else {
      log().info("update {} {} -> {}", state.id, state.value, changeValue.value);
      state.value = changeValue.value;
      changeValue.replyTo.tell(new ChangeValueAck("update", changeValue.id, changeValue.value));
      notifyHttpServer("ping", changeValue.replyTo);
    }
    return this;
  }

  private Behavior<Command> onGetValue(GetValue getValue) {
    log().info("{} -> {}", getValue, state == null ? "(not initialized)" : state);
    if (state == null) {
      getValue.replyTo.tell(new GetValueAckNotFound(getValue.id));
      state = new State(getValue.id, new Value(""));
      notifyHttpServer("start", getValue.replyTo);
    } else {
      getValue.replyTo.tell(new GetValueAck(state.id, state.value));
      notifyHttpServer("ping", getValue.replyTo);
    }
    return this;
  }

  private Behavior<Command> onPassivate() {
    log().info("Stop passivate {} {} {}", entityId, shardId, memberId);
    notifyHttpServer("stop", null);
    return Behaviors.stopped();
  }

  private void notifyHttpServer(String action, ActorRef<Command> sender) {
    //final String address = sender.path().address().getHost().isPresent() ? sender.path().address().toString() : memberId;
    //final String address = sender == null ? null : sender.path().address().toString();
    final var address = sender == null 
      ? null 
      : sender.path().address().getHost().isPresent() 
        ? sender.path().address().toString() 
        : memberId;
    final var entityAction = new EntityAction(memberId, shardId, entityId, action, address);
    final var broadcastEntityAction = new BroadcastEntityAction(entityAction);
    httpServerActorRef.tell(broadcastEntityAction);
  }

  private Logger log() {
    return actorContext.getSystem().log();
  }

  static String entityId(int nodePort, int id) {
    return String.format("%d-%d", nodePort, id);
  }
  
  public interface Command extends CborSerializable {}

  public static class ChangeValue implements Command {
    public final Id id;
    public final Value value;
    public final ActorRef<Command> replyTo;

    @JsonCreator
    public ChangeValue(Id id, Value value, ActorRef<Command> replyTo) {
      this.id = id;
      this.value = value;
      this.replyTo = replyTo;
    }

    @Override
    public String toString() {
      return String.format("%s[%s, %s]", getClass().getSimpleName(), id, value);
    }
  }

  public static class ChangeValueAck implements Command {
    public final String action;
    public final Id id;
    public final Value value;

    @JsonCreator
    public ChangeValueAck(String action, Id id, Value value) {
      this.action = action;
      this.id = id;
      this.value = value;
    }

    @Override
    public String toString() {
      return String.format("%s[%s, %s, %s]", getClass().getSimpleName(), action, id, value);
    }
  }

  public static class GetValue implements Command {
    public final Id id;
    public final ActorRef<Command> replyTo;

    @JsonCreator
    public GetValue(Id id, ActorRef<Command> replyTo) {
      this.id = id;
      this.replyTo = replyTo;
    }

    @Override
    public String toString() {
      return String.format("%s[%s]", getClass().getSimpleName(), id);
    }
  }

  public static class GetValueAck implements Command {
    public final Id id;
    public final Value value;

    @JsonCreator
    public GetValueAck(Id id, Value value) {
      this.id = id;
      this.value = value;
    }

    @Override
    public String toString() {
      return String.format("%s[%s, %s]", getClass().getSimpleName(), id, value);
    }
  }

  public static class GetValueAckNotFound implements Command {
    public final Id id;

    @JsonCreator
    public GetValueAckNotFound(Id id) {
      this.id = id;
    }

    @Override
    public String toString() {
      return String.format("%s[%s]", getClass().getSimpleName(), id);
    }
  }

  public enum Passivate implements Command {
    INSTANCE
  }

  private static class State {
    final Id id;
    Value value;

    public State(Id id, Value value) {
      this.id = id;
      this.value = value;
    }

    @Override
    public String toString() {
      return String.format("%s[%s, %s]", getClass().getSimpleName(), id, value);
    }
  }

  static class Id implements CborSerializable {
    final String id;

    @JsonCreator
    Id(String id) {
      this.id = id;
    }

    @Override
    public String toString() {
      return String.format("%s[%s]", getClass().getSimpleName(), id);
    }
  }

  static class Value implements CborSerializable {
    final Object value;

    @JsonCreator
    Value(Object value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return String.format("%s[%s]", getClass().getSimpleName(), value);
    }
  }
}
