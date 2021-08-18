const singletonAware = {
  clusterStateUpdateNode: function (clusterStateFromNode) {
    const selfPort = clusterStateFromNode.selfPort;

    clusterState.members[selfPort - 2551].singletonAwareStatistics = clusterStateFromNode.singletonAwareStatistics;
    if (selfPort == clusterState.summary.oldest) {
      clusterState.singletonAwareStatistics = clusterStateFromNode.singletonAwareStatistics;
    }
  },

  singletonDetails: function (x, y, w, h) {
    const singletonAwareStatistics = clusterState.singletonAwareStatistics;
    const oldestIsUp = clusterState.summary.oldest >= 2551 ? clusterState.summary.nodes[clusterState.summary.oldest - 2551].state == 'up' : false;

    if (oldestIsUp && singletonAwareStatistics) {
      Label()
        .setX(x)
        .setY(y + 5)
        .setW(w)
        .setH(1)
        .setBorder(0.25)
        .setKey('Singleton Aware')
        .setValue(singletonAwareStatistics.pingRatePs + '/s')
        .setBgColor(color(100, 75))
        .setKeyColor(color(255, 191, 0))
        .setValueColor(color(255))
        .draw();

      Label()
        .setX(x)
        .setY(y + 6)
        .setW(w)
        .setH(1)
        .setBorder(0.25)
        .setKey('Total pings')
        .setValue(singletonAwareStatistics.totalPings.toLocaleString())
        .setKeyColor(color(29, 249, 246))
        .setValueColor(color(255))
        .draw();

      let lineY = y + 7;
      for (let p = 0; p < 9; p++) {
        const port = 2551 + p;
        const node = clusterState.summary.nodes.find((n) => n.port == port);
        const nodePings = singletonAwareStatistics.nodePings[port];
        if (node && nodePings) {
          Label()
            .setX(x)
            .setY(lineY++)
            .setW(w)
            .setH(1)
            .setBorder(0.25)
            .setKey('' + port)
            .setValue(nodePings.toLocaleString())
            .setKeyColor(color(29, 249, 246))
            .setValueColor(color(255))
            .draw();

          const progress = nodePings % 100;
          const length = (w / 100) * (progress == 0 ? 1 : progress);

          strokeWeight(0);

          fill(color(29, 249, 246, 30));
          grid.rect(x, lineY - 0.9, length, 0.7);

          fill(color(249, 49, 46, 100));
          grid.rect(x + length - 0.2, lineY - 0.9, 0.2, 0.7);

          if (node.state != 'up') {
            if (node.state == 'unreachable') {
              fill(color(255, 0, 0, 30));
            } else {
              fill(color(200, 200, 200, 30));
            }
            grid.rect(x - 0.1, lineY - 1, w + 0.2, 1);
          }
        }
      }
    }
  },
};
