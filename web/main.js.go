package web

// This file was generated from main.js.

var main_js string = "var deadline = 0, retry_interval = 0;\nvar ti;\n\nfunction insert(parent, child) {\n  var existing = parent.children();\n  var before = null;\n  existing.each(function () {\n    var jq = $(this);\n    if (jq.attr('name') < child.attr('name')) {\n      before = jq;\n    }\n  });\n  if (before === null) {\n    parent.prepend(child);\n  } else {\n    before.after(child);\n  }\n}\n\nfunction apply(ev) {\n  var parts = ev.Path.split(\"/\")\n  if (parts.length < 2) {\n    return\n  }\n  parts = parts.slice(1); // omit leading empty string\n  var dir_parts = parts.slice(0, parts.length - 1);\n  var dir = $('#root');\n  for (var i = 0; i < dir_parts.length; i++) {\n    var part = dir_parts[i];\n    var next = dir.find('> dl > div[name=\"'+part+'\"] > dd');\n    if (next.length < 1) {\n      var div = $('<div>').attr('name', part);\n      var dd = $('<dd>');\n      div.append($('<dt>').text(part+'/')).append(dd);\n      insert(dir.children('dl'), div);\n      dd.append('<dl>').append('<table><tbody>');\n      next = dd;\n    }\n    dir = next;\n  }\n\n  var basename = parts[parts.length - 1];\n  var entry = dir.find('tr[name=\"'+basename+'\"]');\n  if (entry.length < 1) {\n    var tr = $('<tr class=new>').attr('name', basename);\n    insert(dir.children('table').children('tbody'), tr);\n    tr.append($('<th>').text(basename)).\n      append('<td class=rev>').\n      append('<td class=eq>').\n      append('<td class=body>');\n    entry = tr;\n  }\n  entry.children('td.rev').text('('+ev.Rev+')');\n  entry.children('td.body').text(ev.Body);\n  entry.addClass('new');\n\n  // Kick off the transition in a bit.\n  setTimeout(function() { entry.removeClass('new') }, 550);\n}\n\nfunction time_interval(s) {\n  if (s < 120) return Math.ceil(s) + 's';\n  if (s < 7200) return Math.round(s/60) + 'm';\n  return Math.round(s/3600) + 'h';\n}\n\nfunction countdown() {\n  var body = $('body');\n  var eta = (deadline - new Date().getTime())/1000;\n  if (eta < 0) {\n    body.removeClass('waiting');\n    open();\n  } else {\n    $('#retrymsg').text(\"retrying in \" + time_interval(eta));\n    body.addClass('waiting');\n    ti = setTimeout(countdown, Math.max(100, eta*9));\n  }\n}\n\nfunction retry() {\n  deadline = ((new Date()).getTime()) + retry_interval * 1000;\n  retry_interval += (retry_interval + 5) * (Math.random() + .5);\n  countdown();\n}\n\nfunction open() {\n  var body = $('body');\n  var status = $('#status');\n  status.text(\"connecting\");\n  var ws = new WebSocket(\"ws://\"+location.host+\"/$events\"+path);\n  ws.onmessage = function (ev) {\n    var jev = JSON.parse(ev.data);\n    apply(jev);\n  };\n  ws.onopen = function(ev) {\n    if (retry_interval > 0) {\n      body.addClass('wereback');\n      setTimeout(function () { body.removeClass('wereback') }, 8000);\n    }\n    retry_interval = 0;\n    status.text('open')\n    body.addClass('open').removeClass('loading closed error');\n    $('#root > dl > *, #root > table > tbody > *').remove();\n  };\n  ws.onclose = function(ev) {\n    status.text('closed')\n    body.addClass('closed').removeClass('loading open error wereback');\n    retry();\n  };\n  ws.onerror = function(ev) {\n    status.text('error ' + ev)\n    body.addClass('error').removeClass('loading open closed wereback');\n    retry();\n  };\n}\n\nfunction dr() {\n  $('#trynow').click(function() {\n    clearTimeout(ti);\n    deadline = 0;\n    countdown();\n  });\n\n  if (\"WebSocket\" in window) {\n    open();\n  } else {\n    $('#status').text(\"your browser does not provide websockets\");\n    $('body').addClass('error nows').removeClass('loading open closed wereback');\n  }\n}\n\nfunction jerr() {\n  const m = 'could not load jquery (is your network link down?)';\n  document.getElementById('status').innerText = m;\n  document.getElementsByTagName('body')[0].className = 'error';\n}\n"