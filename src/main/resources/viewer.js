var webSocket;

function sendWebSocketRequest(request) {
  if (webSocket && webSocket.readyState == WebSocket.OPEN) {
    webSocket.send(request);
  } else {
    webSocket = new WebSocket('ws://' + location.host + '/viewer-entities');
    update({ 'name': 'cluster', 'type': 'cluster' });

    webSocket.onopen = function(event) {
      console.log('WebSocket connected', event);
      webSocket.send(request);
    }

    webSocket.onmessage = function(event) {
      console.log(event);
      root = JSON.parse(event.data);
      update(root);
    }

    webSocket.onerror = function(error) {
      console.error('WebSocket error', error);
    }

    webSocket.onclose = function(event) {
      console.log('WebSocket close', event);
    }
  }
}

const chartDiv = document.getElementById('chart');
const width = chartDiv.clientWidth;
const height = chartDiv.clientHeight;
const radius = height / 2;
const tree = d3.tree().size([2 * Math.PI, radius - 75]);

const svg = d3.select('svg')
  .style('width', width)
  .style('height', height)
  .style('padding', '0px')
  .style('box-sizing', 'border-box')
  .style('font', 'sans-serif');

const g = svg.append('g')
  .attr('transform', 'translate(' + width / 2 + ',' + height / 2 + ')');

const gMembers = g.append('g')
  .attr('class', 'members')

const gLink = g.append('g')
  .attr('class', 'links')
  .attr('fill', 'none')
  .attr('stroke', '#555')
  .attr('stroke-opacity', '0.4')
  .attr('stroke-width', 1.5);

const gNode = g.append('g')
  .attr('class', 'nodes')
  .attr('stroke-linejoin', 'round')
  .attr('stroke-width', 3);

sendWebSocketRequest();
setInterval(sendWebSocketRequest, 5000);

function update(hierarchy) {
  updateClusterView(hierarchy);
  updateCropCircle(hierarchy);
}

function updateCropCircle(hierarchy) {
  const t1 = d3.transition()
    .duration(750);

  const t2 = d3.transition()
    .delay(750)
    .duration(750);

  const t3 = d3.transition()
    .delay(1500)
    .duration(750);

  const root = tree(d3.hierarchy(hierarchy));

  const link = gLink.selectAll('path')
    .data(root.links(), linkId);

  const linkEnter = link.enter().append('path')
    .attr('id', d => linkId)
    .attr('class', d => 'link ' + d.source.data.type)
    .style('opacity', 0.000001)
    .attr('d', d3.linkRadial()
                 .angle(d => d.x)
                 .radius(d => d.y));

  link.transition(t2)
    .style('opacity', 1.0)
    .attr('d', d3.linkRadial()
    .angle(d => d.x)
    .radius(d => d.y));

  linkEnter.transition(t3)
    .style('opacity', 1.0);

  link.exit()
    .transition(t1)
    .style('opacity', 0.000001)
    .remove();

  const node = gNode.selectAll('g')
    .data(root.descendants(), nodeId);

  const nodeEnter = node.enter().append('g')
    .attr('id', nodeId)
    .attr('class', d => 'node ' + d.data.type)
    .attr('transform', d => `rotate(${d.x * 180 / Math.PI - 90}) translate(${d.y},0)`)
    .on('mouseover', function() {
      d3.select(this).select('text').style('font-size', 24).style('fill', '#046E97');
    })
    .on('mouseout', function(d) {
      d3.select(this).select('text').style('font-size', 12).style('fill', '#999');
    });

  nodeEnter.append('circle')
    .attr('class', d => d.data.type)
    .attr('fill', circleColor)
    .attr('r', circleRadius)
    .attr('cursor', 'pointer')
    .on('click', clickCircle)
    .style('opacity', 0.000001);

  nodeEnter.append('text')
    .attr('dy', '0.31em')
    .attr('x', labelOffsetX)
    .attr('text-anchor', d => d.x < Math.PI === !d.children ? 'start' : 'end')
    .attr('transform', d => d.x >= Math.PI ? 'rotate(180)' : null)
    .style('opacity', 0.000001)
    .text(d => d.data.name);

  nodeEnter.filter(d => d.data.type.includes('member'))
    .append('text')
    .attr('dy', '0.31em')
    .attr('x', '-6')
    .attr('transform', d => d.x >= Math.PI ? 'rotate(180)' : null)
    .attr('cursor', 'pointer')
    .on('click', clickCircle)
    .style('font-size', 24)
    .style('fill', '#FFF')
    .style('opacity', 1)
    .text(memberNumber);

  nodeEnter.append('title')
    .text(d => d.data.type);

  node.transition(t2)
    .attr('transform', d => `rotate(${d.x * 180 / Math.PI - 90}) translate(${d.y},0)`)
    .select('circle.entity')
      .attr('r', circleRadius)
      .style('fill', entityColor)
      .style('opacity', 1.0);

  node.transition(t2)
    .select('circle.shard')
      .attr('r', circleRadius)
      .style('fill', shardColor)
      .style('opacity', 1.0);

  node.transition(t2)
    .select('circle.member')
      .attr('r', circleRadius)
      .style('fill', circleColor)
      .style('opacity', 1.0);

  node.transition(t2)
    .select('text')
      .style('opacity', 1.0);

  nodeEnter.transition(t3)
    .select('circle')
      .style('opacity', 1.0);

  nodeEnter.transition(t3)
    .select('text')
      .style('opacity', 1.0);

  node.exit()
    .transition(t1)
    .select('circle')
      .attr('r', circleRadiusExit)
      .style('opacity', 0.000001)
      .style('fill', 'red');

  node.exit()
    .transition(t1)
    .select('text')
      .style('opacity', 0.000001);

  node.exit()
    .transition(t1)
    .remove();
}

function updateClusterView(hierarchy) {
  const side = Math.min(width, height) / 20;
  const members = gMembers.selectAll('g')
    .data(memberData(hierarchy));
  
  const membersEnter = members.enter().append('g')
    .attr('cursor', 'pointer')
    .on('click', clickMember);

  membersEnter.append('rect')
    .attr('x', d => d.x)
    .attr('y', d => d.y)
    .attr('width', side)
    .attr('height', side)
    .style('fill', d => d.active ? '#30d35a' : '#555');

  membersEnter.append('text')
    .attr('x', d => d.x + side / 5)
    .attr('y', d => d.y + side / 2)
    .style('font-size', 24)
    .style('fill', '#FFF')
    .text(d => d.memberId - 2550);

  members.select('rect')
    .style('fill', d => d.active ? '#30d35a' : '#555');

  members.select('text');

  function memberData(hierarchy) {
    const members = [];
    let memberId = 2551;
    for (var row = 0; row < 3; row++) {
      for (var col  = 0; col < 3; col++) {
        const x = col * (side + 2) + side / 2 - width / 2;
        const y = row * (side + 2) + side / 2 - height / 2;
        members.push({ memberId: memberId, x: x, y: y, active: isActive(memberId), address: address(memberId) });
        memberId++;
      }
    }
    return members;
  }

  function address(m) {
    const idx = hierarchy.children
      ? hierarchy.children.findIndex(d => d.name.endsWith(m))
      : -1;
    return idx >= 0 ? hierarchy.children[idx].name : '';
  }

  function isActive(m) {
    return hierarchy.children
      ? hierarchy.children.findIndex(d => d.name.endsWith(m)) >= 0
      : false;
  }
}

function linkId(d) {
  return d.source.data.name + '-' + d.target.data.name;
}

function nodeId(d) {
  return d.data.type + '-' + d.data.name;
}

function entityColor(d) {
  return d.data.name == traceEntityId ? '#FF0000' : '#42aaff';
}

function shardColor(d) {
  return d.data.name == traceShardId ? '#FF0000' : '#00C000';
}

function circleColor(d) {
  if (d.data.type.includes('entity')) {
    return d.data.name == traceEntityId ? '#AA0000' : '#046E97';
  } else if (d.data.type.includes('shard')) {
    return d.data.name == traceShardId ? '#AA0000' : '#00C000';
  } else if (d.data.type.includes('singleton')) {
    return '#8F42EB';
  } else if (d.data.type.includes('httpServer')) {
    return '#F3B500';
  } else if (d.data.type.includes('member')) {
    return '#F17D00';
  } else if (d.data.type.includes('cluster')) {
    return '#B30000';
  } else {
    return 'red';
  }
}

function circleRadius(d) {
  if (d.data.type.includes('entity')) {
    return 8;
  } else if (d.data.type.includes('shard')) {
    return 12;
  } else if (d.data.type.includes('member')) {
    return 22;
  } else if (d.data.type.includes('cluster')) {
    return 10;
  } else {
    return 3;
  }
}

function circleRadiusExit(d) {
  return 4 * circleRadius(d);
}

function labelOffsetX(d) {
  if (d.data.type.includes('entity')) {
    return offset(d, 10);
  } else if (d.data.type.includes('shard')) {
    return offset(d, 14);
  } else if (d.data.type.includes('member')) {
    return offset(d, 24);
  } else if (d.data.type.includes('cluster')) {
    return offset(d, 12);
  } else {
    return offset(d, 5);
  }

  function offset(d, distance) {
    return d.x < Math.PI === !d.children ? distance : -distance;
  }
}

function memberNumber(d) {
  return d.data.name.slice(-1);
}

function clickCircle(d) {
  if (d.data.type.indexOf('member') >= 0) {
    sendWebSocketRequest(d.data.name);
  } else if (d.data.type == 'entity') {
    traceEntityId = d.data.name == traceEntityId ? '' : d.data.name;
    traceShardId = traceEntityId.length > 0 ? d.parent.data.name : '';
  } else if (d.data.type == 'shard') {
    traceShardId = d.data.name == traceShardId ? '' : d.data.name;
  }
}

function clickMember(d) {
  sendWebSocketRequest(d.address);
}

let traceEntityIdNew = '';
let traceEntityId = '';
let traceShardId = '';

d3.select('body').on('keydown', function () {
  if ((d3.event.key >= '0' && d3.event.key <= '9') || d3.event.key == '-') {
    traceEntityIdNew += d3.event.key;
  } else if (d3.event.key == 'Enter') {
    traceEntityId = traceEntityIdNew;
    traceEntityIdNew = '';
  }
});