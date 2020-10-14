package cluster;

import java.time.Duration;
import java.util.Date;

import org.slf4j.Logger;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.javadsl.TimerScheduler;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.EntityRef;
import cluster.EntityActor.Command;

class EntityCommandActor extends AbstractBehavior<EntityActor.Command> {
  private final ActorContext<EntityActor.Command> actorContext;
  private final ClusterSharding clusterSharding;
  private final int entitiesPerNode;
  private final Integer nodePort;

  static Behavior<EntityActor.Command> create() {
    return Behaviors.setup(actorContext -> 
        Behaviors.withTimers(timer -> new EntityCommandActor(actorContext, timer)));
  }

  private EntityCommandActor(ActorContext<Command> actorContext, TimerScheduler<EntityActor.Command> timerScheduler) {
    super(actorContext);
    this.actorContext = actorContext;
    clusterSharding = ClusterSharding.get(actorContext.getSystem());

    entitiesPerNode = actorContext.getSystem().settings().config().getInt("entity-actor.entities-per-node");
    final Duration interval = Duration.parse(actorContext.getSystem().settings().config().getString("entity-actor.command-tick-interval-iso-8601"));
    timerScheduler.startTimerWithFixedDelay(Tick.ticktock, interval);
    nodePort = actorContext.getSystem().address().getPort().orElse(-1);
  }

  @Override
  public Receive<Command> createReceive() {
    return newReceiveBuilder()
        .onMessage(Tick.class, t -> onTick())
        .onMessage(EntityActor.ChangeValueAck.class, this::onChangeValueAck)
        .build();
  }

  private Behavior<EntityActor.Command> onTick() {
    final String entityId = String.format("node-%d-%d", nodePort, Math.round(Math.random() * entitiesPerNode));
    final EntityActor.Id id = new EntityActor.Id(entityId);
    final EntityActor.Value value = new EntityActor.Value(new Date());
    final EntityRef<Command> entityRef = clusterSharding.entityRefFor(EntityActor.entityTypeKey, entityId);
    entityRef.tell(new EntityActor.ChangeValue(id, value, actorContext.getSelf()));
    return this;
  }

  private Behavior<EntityActor.Command> onChangeValueAck(EntityActor.ChangeValueAck changeValueAck) {
    log().info("{}", changeValueAck);
    return this;
  }

  private Logger log() {
    return actorContext.getSystem().log();
  }

  enum Tick implements EntityActor.Command {
    ticktock
  }
}
