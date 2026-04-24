import Foundation
import Vision
import AppKit

func usage() -> Never {
    fputs("Usage: ocr_crop.swift <image-path> <x> <y> <w> <h>\n", stderr)
    fputs("Coordinates are normalized 0..1 from bottom-left.\n", stderr)
    exit(1)
}

guard CommandLine.arguments.count == 6 else {
    usage()
}

let path = CommandLine.arguments[1]

func parse(_ value: String) -> CGFloat {
    guard let parsed = Double(value) else {
        usage()
    }
    return CGFloat(parsed)
}

let rect = CGRect(
    x: parse(CommandLine.arguments[2]),
    y: parse(CommandLine.arguments[3]),
    width: parse(CommandLine.arguments[4]),
    height: parse(CommandLine.arguments[5])
)

guard rect.minX >= 0, rect.minY >= 0, rect.maxX <= 1, rect.maxY <= 1 else {
    fputs("Crop rect must stay within 0..1 bounds.\n", stderr)
    exit(2)
}

let url = URL(fileURLWithPath: path)

guard let image = NSImage(contentsOf: url) else {
    fputs("Could not open image: \(path)\n", stderr)
    exit(3)
}

guard
    let tiff = image.tiffRepresentation,
    let bitmap = NSBitmapImageRep(data: tiff),
    let cgImage = bitmap.cgImage
else {
    fputs("Could not convert image: \(path)\n", stderr)
    exit(4)
}

let pxRect = CGRect(
    x: rect.origin.x * CGFloat(cgImage.width),
    // Vision bounding boxes are bottom-left based; CGImage cropping is top-left based.
    y: (1 - rect.maxY) * CGFloat(cgImage.height),
    width: rect.size.width * CGFloat(cgImage.width),
    height: rect.size.height * CGFloat(cgImage.height)
).integral

guard let cropped = cgImage.cropping(to: pxRect) else {
    fputs("Could not crop image.\n", stderr)
    exit(5)
}

let request = VNRecognizeTextRequest()
request.recognitionLevel = .accurate
request.usesLanguageCorrection = false
request.recognitionLanguages = ["it-IT", "en-US"]

let handler = VNImageRequestHandler(cgImage: cropped, options: [:])

do {
    try handler.perform([request])
    let observations = request.results ?? []
    let output = observations.compactMap { obs -> [String: Any]? in
        guard let candidate = obs.topCandidates(1).first else { return nil }
        let box = obs.boundingBox
        return [
            "text": candidate.string,
            "confidence": candidate.confidence,
            "x": box.origin.x,
            "y": box.origin.y,
            "w": box.size.width,
            "h": box.size.height
        ]
    }
    let data = try JSONSerialization.data(withJSONObject: output, options: [.prettyPrinted, .sortedKeys])
    FileHandle.standardOutput.write(data)
} catch {
    fputs("OCR failed: \(error)\n", stderr)
    exit(6)
}
