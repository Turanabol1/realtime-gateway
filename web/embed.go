// Package web містить статичний вебклієнт, вбудований у виконуваний файл.
package web

import "embed"

// Files — вміст каталогу web (index.html), який віддається через http.FileServer.
//
//go:embed index.html
var Files embed.FS
