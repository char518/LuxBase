// Renders the DMG installer background at 2x (1320x840 px for a 660x420 pt window) with the
// LuxBase palette: navy field, blue/teal glows, dot grid, brand mark, drag arrow and honest
// install/Gatekeeper/migration copy. Run: swiftc make-dmg-background.swift -o /tmp/luxbg && /tmp/luxbg <out.png>
import AppKit

let W: CGFloat = 660
let H: CGFloat = 420
let out = CommandLine.arguments.count > 1 ? CommandLine.arguments[1] : "src-tauri/dmg-background.png"

guard let rep = NSBitmapImageRep(
  bitmapDataPlanes: nil, pixelsWide: Int(W * 2), pixelsHigh: Int(H * 2),
  bitsPerSample: 8, samplesPerPixel: 4, hasAlpha: true, isPlanar: false,
  colorSpaceName: .deviceRGB, bytesPerRow: 0, bitsPerPixel: 0
) else { fatalError("bitmap rep") }
rep.size = NSSize(width: W, height: H)
NSGraphicsContext.saveGraphicsState()
NSGraphicsContext.current = NSGraphicsContext(bitmapImageRep: rep)

func rgb(_ hex: UInt32, _ a: CGFloat = 1) -> NSColor {
  NSColor(srgbRed: CGFloat((hex >> 16) & 255) / 255, green: CGFloat((hex >> 8) & 255) / 255, blue: CGFloat(hex & 255) / 255, alpha: a)
}
// Top-left coordinate helpers over the non-flipped bitmap context.
func rect(_ x: CGFloat, _ y: CGFloat, _ w: CGFloat, _ h: CGFloat) -> NSRect { NSRect(x: x, y: H - y - h, width: w, height: h) }
func pt(_ x: CGFloat, _ y: CGFloat) -> NSPoint { NSPoint(x: x, y: H - y) }
func text(_ s: String, centerX: CGFloat, topY: CGFloat, size: CGFloat, weight: NSFont.Weight, color: NSColor) {
  let font = NSFont.systemFont(ofSize: size, weight: weight)
  let attrs: [NSAttributedString.Key: Any] = [.font: font, .foregroundColor: color]
  let str = NSAttributedString(string: s, attributes: attrs)
  let sz = str.size()
  str.draw(at: NSPoint(x: centerX - sz.width / 2, y: H - topY - sz.height))
}

// Field: deep navy diagonal gradient plus two palette glows and a faint dot grid.
NSGradient(colors: [rgb(0x0b1120), rgb(0x15223c)])!.draw(in: rect(0, 0, W, H), angle: -70)
NSGradient(colors: [rgb(0x176df5, 0.34), rgb(0x176df5, 0)])!
  .draw(fromCenter: pt(120, 70), radius: 0, toCenter: pt(120, 70), radius: 280, options: [])
NSGradient(colors: [rgb(0x0e9b91, 0.22), rgb(0x0e9b91, 0)])!
  .draw(fromCenter: pt(560, 350), radius: 0, toCenter: pt(560, 350), radius: 250, options: [])
let dot = rgb(0xe8eef6, 0.05)
for gx in stride(from: CGFloat(20), to: W, by: 22) {
  for gy in stride(from: CGFloat(20), to: H, by: 22) {
    dot.setFill()
    NSBezierPath(ovalIn: rect(gx, gy, 1.4, 1.4)).fill()
  }
}

// Brand mark: rounded blue plate with three white bars, matching the application icon.
let plate = NSBezierPath(roundedRect: rect(28, 24, 40, 40), xRadius: 10, yRadius: 10)
NSGradient(colors: [rgb(0x4f95ff), rgb(0x176df5)])!.draw(in: plate, angle: -90)
rgb(0xffffff, 0.95).setFill()
for (i, h) in [CGFloat(10), 16, 22].enumerated() {
  NSBezierPath(roundedRect: rect(38 + CGFloat(i) * 8, 24 + 30 - h, 5, h), xRadius: 1.5, yRadius: 1.5).fill()
}
text("LuxBase", centerX: 122, topY: 28, size: 24, weight: .bold, color: rgb(0xf2f6fc))
text("多协议数据库客户端 · MySQL / PostgreSQL / SQLite / Redis", centerX: 262, topY: 58, size: 10.5, weight: .regular, color: rgb(0x9fb0c8))

// Drag arrow between the two icon slots (positions mirror bundle.macOS.dmg in tauri.conf.json).
let arrow = NSBezierPath()
arrow.move(to: pt(240, 140))
arrow.line(to: pt(410, 140))
arrow.lineWidth = 2
arrow.setLineDash([6, 5], count: 2, phase: 0)
rgb(0xe8eef6, 0.55).setStroke()
arrow.stroke()
let head = NSBezierPath()
head.move(to: pt(420, 140))
head.line(to: pt(406, 133))
head.line(to: pt(406, 147))
head.close()
rgb(0xe8eef6, 0.75).setFill()
head.fill()
text("拖拽安装", centerX: 330, topY: 112, size: 11, weight: .medium, color: rgb(0xc6d4e8))

// Honest footer: what to do, what Gatekeeper may say, and what happens to old data.
text("把 LuxBase 拖入 Applications 文件夹即完成安装", centerX: 330, topY: 300, size: 12.5, weight: .semibold, color: rgb(0xeaf1fa))
text("开发者构建 · 若被 Gatekeeper 拦截：右键图标 → 打开", centerX: 330, topY: 326, size: 10, weight: .regular, color: rgb(0x9fb0c8))
text("连接与凭据仅存本机 · 首次启动自动复制迁移旧版 Arcbase Studio 数据", centerX: 330, topY: 348, size: 9, weight: .regular, color: rgb(0x7688a2))

NSGraphicsContext.restoreGraphicsState()
guard let data = rep.representation(using: .png, properties: [:]) else { fatalError("png") }
try! data.write(to: URL(fileURLWithPath: out))
print("wrote \(out)")