package cluster;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import org.slf4j.Logger;

class HttpServerActor {
  private final ActorContext<HttpServer.Statistics> context;
  private final HttpServer httpServer;

  HttpServerActor(ActorContext<HttpServer.Statistics> context) {
    this.context = context;

    httpServer = HttpServer.start(context.getSystem());
  }

  static Behavior<HttpServer.Statistics> create() {
    return Behaviors.setup(context -> new HttpServerActor(context).behavior());
  }

  private Behavior<HttpServer.Statistics> behavior() {
    return Behaviors.receive(HttpServer.Statistics.class)
        .onMessage(HttpServer.ClusterAwareStatistics.class, this::onClusterAwareStatistics)
        .onMessage(HttpServer.SingletonAwareStatistics.class, this::omSingletonAwareStatistics)
        .build();
  }

  private Behavior<HttpServer.Statistics> onClusterAwareStatistics(HttpServer.ClusterAwareStatistics clusterAwareStatistics) {
    log().info("Cluster aware statistics {} {}", clusterAwareStatistics.totalPings, clusterAwareStatistics.nodePings);
    httpServer.load(clusterAwareStatistics);
    return Behaviors.same();
  }

  private Behavior<HttpServer.Statistics> omSingletonAwareStatistics(HttpServer.SingletonAwareStatistics singletonAwareStatistics) {
    log().info("Singleton aware statistics {}", singletonAwareStatistics.nodePings);
    httpServer.load(singletonAwareStatistics);
    return Behaviors.same();
  }

  private Logger log() {
    return context.getLog();
  }
}
