package cluster;

import java.time.Duration;

import org.slf4j.Logger;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.javadsl.TimerScheduler;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import cluster.EntityActor.Command;

class EntityQueryActor extends AbstractBehavior<EntityActor.Command> {
  private final ActorContext<EntityActor.Command> actorContext;
  private final ClusterSharding clusterSharding;
  private final int entitiesPerNode;
  private final Integer nodePort;

  static Behavior<EntityActor.Command> create() {
    return Behaviors.setup(actorContext -> 
        Behaviors.withTimers(timer -> new EntityQueryActor(actorContext, timer)));
  }

  private EntityQueryActor(ActorContext<Command> actorContext, TimerScheduler<EntityActor.Command> timerScheduler) {
    super(actorContext);
    this.actorContext = actorContext;
    clusterSharding = ClusterSharding.get(actorContext.getSystem());

    entitiesPerNode = actorContext.getSystem().settings().config().getInt("entity-actor.entities-per-node");
    final var interval = Duration.parse(actorContext.getSystem().settings().config().getString("entity-actor.query-tick-interval-iso-8601"));
    timerScheduler.startTimerWithFixedDelay(Tick.ticktock, interval);
    nodePort = actorContext.getSystem().address().getPort().orElse(-1);
  }

  @Override
  public Receive<Command> createReceive() {
    return newReceiveBuilder()
        .onMessage(Tick.class, t -> onTick())
        .onMessage(EntityActor.GetValueAck.class, this::onGetValueAck)
        .onMessage(EntityActor.GetValueAckNotFound.class, this::onGetValueAckNotFound)
        .build();
  }

  private Behavior<EntityActor.Command> onTick() {
    final var entityId = EntityActor.entityId(nodePort, (int) Math.round(Math.random() * entitiesPerNode));
    final var id = new EntityActor.Id(entityId);
    final var entityRef = clusterSharding.entityRefFor(EntityActor.entityTypeKey, entityId);
    entityRef.tell(new EntityActor.GetValue(id, actorContext.getSelf()));
    return this;
  }

  private Behavior<EntityActor.Command> onGetValueAck(EntityActor.GetValueAck getValueAck) {
    log().info("{}", getValueAck);
    return this;
  }

  private Behavior<EntityActor.Command> onGetValueAckNotFound(EntityActor.GetValueAckNotFound getValueAckNotFound) {
    log().info("{}", getValueAckNotFound);
    return this;
  }
    
  private Logger log() {
    return actorContext.getSystem().log();
  }

  enum Tick implements EntityActor.Command {
    ticktock
  }
}
