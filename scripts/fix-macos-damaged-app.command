#!/bin/zsh
set -euo pipefail

APP_PATH="/Applications/Other Model.app"

if [[ ! -d "$APP_PATH" ]]; then
  echo "没有找到 $APP_PATH"
  echo "请先把 Other Model.app 拖到 /Applications 后再运行本脚本。"
  read -k 1 "?按任意键退出..."
  exit 1
fi

echo "正在修复 macOS Gatekeeper 隔离属性：$APP_PATH"
/usr/bin/xattr -cr "$APP_PATH" 2>/dev/null || /usr/bin/sudo /usr/bin/xattr -cr "$APP_PATH"

if /usr/bin/codesign --verify --deep --strict "$APP_PATH" >/dev/null 2>&1; then
  echo "签名校验通过。"
else
  echo "正在重新进行本机 ad-hoc 签名..."
  /usr/bin/codesign --force --deep --sign - "$APP_PATH"
fi

/usr/bin/xattr -cr "$APP_PATH" 2>/dev/null || true

echo "修复完成，正在打开 Other Model..."
/usr/bin/open "$APP_PATH"

echo "如果仍然提示已损坏，请把下面输出发给开发者："
/usr/sbin/spctl -a -vvv -t exec "$APP_PATH" 2>&1 || true
read -k 1 "?按任意键关闭窗口..."
