package cluster;

import java.time.Duration;

import org.slf4j.Logger;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.javadsl.TimerScheduler;
import cluster.EntityActor.Command;

class EntityQueryActor extends AbstractBehavior<EntityActor.Command> {
  private final ActorContext<EntityActor.Command> actorContext;
  private final int entitiesPerNode;

  static Behavior<EntityActor.Command> create() {
    return Behaviors.setup(actorContext -> 
        Behaviors.withTimers(timer -> new EntityQueryActor(actorContext, timer)));
  }

  private EntityQueryActor(ActorContext<Command> actorContext, TimerScheduler<EntityActor.Command> timerScheduler) {
    super(actorContext);
    this.actorContext = actorContext;

    entitiesPerNode = actorContext.getSystem().settings().config().getInt("entity-actor.entities-per-node");
    final Duration interval = Duration.parse(actorContext.getSystem().settings().config().getString("entity-actor.query-tick-interval-iso-8601"));
    timerScheduler.startTimerWithFixedDelay(Tick.ticktock, interval);
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
