const clusterAware = {
  clusterStateUpdateNode: function (clusterStateFromNode) {
    const selfPort = clusterStateFromNode.selfPort;

    clusterState.members[selfPort - 2551].clusterAwareStatistics = clusterStateFromNode.clusterAwareStatistics;
  },

  nodeDetails: function (x, y, w, h, nodeNo) {
    const selfPort = 2551 + nodeNo;
    const clusterAwareStatistics = clusterState.members[nodeNo].clusterAwareStatistics;

    if (clusterAwareStatistics) {
      Label()
        .setX(x)
        .setY(y + 2)
        .setW(9)
        .setH(1)
        .setBorder(0.25)
        .setKey('Cluster Aware')
        .setValue(clusterAwareStatistics.pingRatePs + '/s')
        .setBgColor(color(100, 75))
        .setKeyColor(color(255, 191, 0))
        .setValueColor(color(255))
        .draw();

      Label()
        .setX(x)
        .setY(y + 3)
        .setW(9)
        .setH(1)
        .setBorder(0.25)
        .setKey('Total pings')
        .setValue(clusterAwareStatistics.totalPings.toLocaleString())
        .setKeyColor(color(29, 249, 246))
        .setValueColor(color(255))
        .draw();

      let lineY = y + 4;
      for (let p = 0; p < 9; p++) {
        const port = 2551 + p;
        const nodePings = clusterAwareStatistics.nodePings[port];
        if (nodePings && port != selfPort) {
          Label()
            .setX(x)
            .setY(lineY++)
            .setW(9)
            .setH(1)
            .setBorder(0.25)
            .setKey('' + port)
            .setValue(nodePings.toLocaleString())
            .setKeyColor(color(29, 249, 246))
            .setValueColor(color(255))
            .draw();

          const progress = nodePings % 100;
          const length = (9 / 100) * (progress == 0 ? 1 : progress);

          strokeWeight(0);
          fill(color(29, 249, 246, 30));
          grid.rect(x, lineY - 0.9, length, 0.7);

          fill(color(249, 49, 46, 100));
          grid.rect(x + length - 0.2, lineY - 0.9, 0.2, 0.7);
        }
      }
    }
  },
};
