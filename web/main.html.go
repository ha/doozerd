package web

// This file was generated from web/main.html.

var main_html string = "<html>\n  <head>\n    <title>{{ .Name }} {{ .Path }} doozer viewer</title>\n    <link rel=stylesheet href=/$main.css>\n  </head>\n\n  <body class=loading>\n    <div id=info>\n      <span id=status>loading</span>\n      <span id=waiting class=msg>\n        <span id=retrymsg></span>\n        <a id=trynow>[Try now]</a>\n      </span>\n      <span id=wereback class=msg>...and, we're back!</span>\n    </div>\n\n    <dl id=tree>\n      <dt>{{ .Path }}</dt>\n      <dd id=root>\n        <dl></dl>\n        <table><tbody></table>\n      </dd>\n    </dl>\n\n    <script>\n      var path = \"{{ .Path }}\";\n    </script>\n    <script src=/$main.js></script>\n    <script src=\"http://ajax.googleapis.com/ajax/libs/jquery/1.4.2/jquery.min.js\" async defer onload=$(document).ready(dr) onerror=jerr()></script>\n  </body>\n</html>\n"
