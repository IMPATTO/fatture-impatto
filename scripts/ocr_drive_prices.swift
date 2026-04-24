import Foundation
import Vision
import AppKit

func usage() -> Never {
    fputs("Usage: ocr_drive_prices.swift <image-path>\n", stderr)
    exit(1)
}

guard CommandLine.arguments.count >= 2 else {
    usage()
}

let path = CommandLine.arguments[1]
let url = URL(fileURLWithPath: path)

guard let image = NSImage(contentsOf: url) else {
    fputs("Could not open image: \(path)\n", stderr)
    exit(2)
}

guard
    let tiff = image.tiffRepresentation,
    let bitmap = NSBitmapImageRep(data: tiff),
    let cgImage = bitmap.cgImage
else {
    fputs("Could not convert image: \(path)\n", stderr)
    exit(3)
}

let request = VNRecognizeTextRequest()
request.recognitionLevel = .accurate
request.usesLanguageCorrection = false
request.recognitionLanguages = ["it-IT", "en-US"]

let handler = VNImageRequestHandler(cgImage: cgImage, options: [:])

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
    exit(4)
}
