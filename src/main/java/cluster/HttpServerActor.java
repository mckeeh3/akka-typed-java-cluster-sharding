package cluster;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import cluster.HttpServer.EntityAction;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;

import org.slf4j.Logger;

class HttpServerActor {
  private final ActorContext<HttpServer.Statistics> actorContext;
  private final HttpServer httpServer;
  private Set<ActorRef<HttpServer.Statistics>> serviceInstances;
  private static final ServiceKey<HttpServer.Statistics> serviceKey = 
    ServiceKey.create(HttpServer.Statistics.class, HttpServer.class.getSimpleName());

  static Behavior<HttpServer.Statistics> create() {
    return Behaviors.setup(context -> new HttpServerActor(context).behavior());
  }

  private HttpServerActor(ActorContext<HttpServer.Statistics> actorContext) {
    this.actorContext = actorContext;

    receptionistRegisterSubscribe(actorContext);
    httpServer = HttpServer.start(actorContext.getSystem());
  }

  private Behavior<HttpServer.Statistics> behavior() {
    return Behaviors.receive(HttpServer.Statistics.class)
        .onMessage(Listeners.class, this::onListeners)
        .onMessage(HttpServer.ClusterAwareStatistics.class, this::onClusterAwareStatistics)
        .onMessage(HttpServer.SingletonAwareStatistics.class, this::onSingletonAwareStatistics)
        .onMessage(BroadcastEntityAction.class, this::onBroadcastEntityAction)
        .onMessage(HttpServer.EntityAction.class, this::onNotifyEntityAction)
        .build();
  }

  private Behavior<HttpServer.Statistics> onListeners(Listeners listeners) {
    serviceInstances = listeners.listing.getAllServiceInstances(serviceKey);
    return Behaviors.same();
  }

  private Behavior<HttpServer.Statistics> onClusterAwareStatistics(HttpServer.ClusterAwareStatistics clusterAwareStatistics) {
    log().info("Cluster aware statistics {} {}", clusterAwareStatistics.totalPings, clusterAwareStatistics.nodePings);
    httpServer.load(clusterAwareStatistics);
    return Behaviors.same();
  }

  private Behavior<HttpServer.Statistics> onSingletonAwareStatistics(HttpServer.SingletonAwareStatistics singletonAwareStatistics) {
    log().info("Singleton aware statistics {}", singletonAwareStatistics.nodePings);
    httpServer.load(singletonAwareStatistics);
    return Behaviors.same();
  }

  private Behavior<HttpServer.Statistics> onBroadcastEntityAction(BroadcastEntityAction broadcastEntityAction) {
    serviceInstances.stream()
        .forEach(httpServerActorRef -> httpServerActorRef.tell(broadcastEntityAction.entityAction));
    return Behaviors.same();
  }

  private Behavior<HttpServer.Statistics> onNotifyEntityAction(HttpServer.EntityAction entityAction) {
    log().info("{}", entityAction);
    httpServer.load(entityAction);
    return Behaviors.same();
  }

  private Logger log() {
    return actorContext.getLog();
  }

  private static void receptionistRegisterSubscribe(ActorContext<HttpServer.Statistics> actorContext) {
    final var listingActorRef = actorContext.messageAdapter(Receptionist.Listing.class, Listeners::new);

    actorContext.getSystem().receptionist().tell(Receptionist.register(serviceKey, actorContext.getSelf()));
    actorContext.getSystem().receptionist().tell(Receptionist.subscribe(serviceKey, listingActorRef));
  }

  private static class Listeners implements HttpServer.Statistics {
    final Receptionist.Listing listing;

    private Listeners(Receptionist.Listing listing) {
      this.listing = listing;
    }
  }

  public static class BroadcastEntityAction implements HttpServer.Statistics {
    public final HttpServer.EntityAction entityAction;

    @JsonCreator
    public BroadcastEntityAction(EntityAction entityAction) {
      this.entityAction = entityAction;
    }

    @Override
    public String toString() {
      return String.format("%s[%s]", getClass().getSimpleName(), entityAction);
    }
  }
}
