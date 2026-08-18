package main

import "log/slog"

func errAttr(err error) slog.Attr { return slog.Any("error", err) }
