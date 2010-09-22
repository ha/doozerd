var deadline = 0, retry_interval = 0;
var ti;

function insert(parent, child) {
  var existing = parent.children();
  var before = null;
  existing.each(function () {
    var jq = $(this);
    if (jq.attr('name') < child.attr('name')) {
      before = jq;
    }
  });
  if (before === null) {
    parent.prepend(child);
  } else {
    before.after(child);
  }
}

function apply(ev) {
  var parts = ev.Path.split("/")
  if (parts.length < 2) {
    return
  }
  parts = parts.slice(1); // omit leading empty string
  var dir_parts = parts.slice(0, parts.length - 1);
  var dir = $('#root');
  for (var i = 0; i < dir_parts.length; i++) {
    var part = dir_parts[i];
    var next = dir.find('div[name="'+part+'"] > dd');
    if (next.length < 1) {
      var div = $('<div>').attr('name', part);
      var dd = $('<dd>');
      div.append($('<dt>').text(part+'/')).append(dd);
      insert(dir.children('dl'), div);
      dd.append('<dl>').append('<table><tbody>');
      next = dd;
    }
    dir = next;
  }

  var basename = parts[parts.length - 1];
  var entry = dir.find('tr[name="'+basename+'"]');
  if (entry.length < 1) {
    var tr = $('<tr>').attr('name', basename);
    insert(dir.children('table').children('tbody'), tr);
    tr.append($('<th>').text(basename)).
      append('<td class=cas>').
      append('<td class=eq>').
      append('<td class=body>');
    entry = tr;
  }
  entry.children('td.cas').text('('+ev.Cas+')');
  entry.children('td.body').text(ev.Body);
}

function time_interval(s) {
  if (s < 120) return Math.ceil(s) + 's';
  if (s < 7200) return Math.round(s/60) + 'm';
  return Math.round(s/3600) + 'h';
}

function countdown() {
  var body = $('body');
  var eta = (deadline - new Date().getTime())/1000;
  if (eta < 0) {
    body.removeClass('waiting');
    open();
  } else {
    $('#retrymsg').text("retrying in " + time_interval(eta));
    body.addClass('waiting');
    ti = setTimeout(countdown, Math.max(100, eta*9));
  }
}

function retry() {
  deadline = ((new Date()).getTime()) + retry_interval * 1000;
  retry_interval += (retry_interval + 5) * (Math.random() + .5);
  countdown();
}

function open() {
  var body = $('body');
  var status = $('#status');
  status.text("connecting");
  var ws = new WebSocket("ws://"+location.host+"/all");
  ws.onmessage = function (ev) {
    var jev = JSON.parse(ev.data);
    apply(jev);
  };
  ws.onopen = function(ev) {
    if (retry_interval > 0) {
      body.addClass('wereback');
      setTimeout(function () { body.removeClass('wereback') }, 8000);
    }
    retry_interval = 0;
    status.text('open')
    body.addClass('open').removeClass('loading closed error');
    $('#root > dl > *, #root > table > tbody > *').remove();
  };
  ws.onclose = function(ev) {
    status.text('closed')
    body.addClass('closed').removeClass('loading open error wereback');
    retry();
  };
  ws.onerror = function(ev) {
    status.text('error ' + ev)
    body.addClass('error').removeClass('loading open closed wereback');
    retry();
  };
}

function dr() {
  $('#trynow').click(function() {
    clearTimeout(ti);
    deadline = 0;
    countdown();
  });

  if ("WebSocket" in window) {
    open();
  } else {
    $('#status').text("no websockets");
    $('body').addClass('error nows').removeClass('loading open closed wereback');
  }
}
