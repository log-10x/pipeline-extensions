package com.log10x.ext.edge.output.dev;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.log10x.api.util.MapperUtil;

/**
 * An output stream that accumulates TenXObject summaries, templates and
 * encoded events in memory, then on close produces a compact JSON payload
 * with a shareable console URL.
 *
 * Two output entries in stream.yaml (summary, event) share a single
 * instance of this stream via the 'group' mechanism. Each entry uses
 * a JSON 'type' field (S, E) to tag the data type.
 *
 * For configuration, see:
 * {@link https://doc.log10x.com/run/output/event/dev/}
 */
public class DevOutputStream extends ByteArrayOutputStream {

	private static final Logger logger = LogManager.getLogger(DevOutputStream.class);

	private static final String CONSOLE_URL = "https://console.log10x.com";

	// --- Options (populated from config via MapperUtil) ---

	public static class DevOutputOptions {

		public final boolean devOutputEnabled;
		public final String devOutputBaseUrl;
		public final boolean devOutputOpenBrowser;
		public final boolean devOutputLocalOnly;
		public final String devOutputFilePath;
		public final String devOutputSourceName;
		public final double devOutputDailyGb;
		public final int devOutputTopPatterns;
		public final int devOutputVrTemplates;
		public final int devOutputVrEventsPerTemplate;

		public DevOutputOptions(
			boolean devOutputEnabled,
			String devOutputBaseUrl,
			boolean devOutputOpenBrowser,
			boolean devOutputLocalOnly,
			String devOutputFilePath,
			String devOutputSourceName,
			double devOutputDailyGb,
			int devOutputTopPatterns,
			int devOutputVrTemplates,
			int devOutputVrEventsPerTemplate) {

			this.devOutputEnabled = devOutputEnabled;
			this.devOutputBaseUrl = devOutputBaseUrl;
			this.devOutputOpenBrowser = devOutputOpenBrowser;
			this.devOutputLocalOnly = devOutputLocalOnly;
			this.devOutputFilePath = devOutputFilePath;
			this.devOutputSourceName = devOutputSourceName;
			this.devOutputDailyGb = devOutputDailyGb;
			this.devOutputTopPatterns = devOutputTopPatterns;
			this.devOutputVrTemplates = devOutputVrTemplates;
			this.devOutputVrEventsPerTemplate = devOutputVrEventsPerTemplate;
		}

		protected DevOutputOptions() {
			this(true, null, false, false, null, "your logs", 100, 10, 10, 1);
		}
	}

	private final DevOutputOptions options;
	private final ObjectMapper mapper = MapperUtil.noFailUnknownJsonMapper;

	// --- Accumulated data ---

	private final List<JsonNode> summaries = new ArrayList<>();
	private final Map<String, String> templatesByHash = new LinkedHashMap<>();

	// Encoded event stats — bounded by number of unique templates, not events
	private long totalEncodedBytes = 0;
	private final Map<String, Integer> eventCountsByHash = new HashMap<>();
	private final Map<String, Long> encSizesByHash = new HashMap<>();

	// VR samples — only keep a few encoded events per template hash
	private final Map<String, List<String>> vrSamplesByHash = new HashMap<>();

	// --- Constructor ---

	public DevOutputStream(Map<String, Object> args) {

		this(MapperUtil.noFailUnknownJsonMapper.convertValue(args,
			DevOutputOptions.class));
	}

	public DevOutputStream(DevOutputOptions options) {
		this.options = options;
	}

	// --- Stream overrides ---

	@Override
	public synchronized void flush() throws IOException {

		if (this.count > 0) {

			String line = new String(this.buf, 0, this.count, StandardCharsets.UTF_8).trim();
			this.reset();

			if (!line.isEmpty()) {
				processLine(line);
			}
		}

		super.flush();
	}

	// --- Line processing ---

	private void processLine(String line) {

		if (line.length() < 2) {
			return;
		}

		// All lines are JSON with a "type" field
		if (line.charAt(0) != '{') {
			if (logger.isDebugEnabled()) {
				logger.debug("Unexpected non-JSON line: " + line.substring(0, Math.min(20, line.length())));
			}
			return;
		}

		try {
			JsonNode node = mapper.readTree(line);
			String type = node.path("type").asText("");

			switch (type) {
				case "S":
					processSummary(node);
					break;
				case "E":
					processEvent(node);
					break;
				default:
					if (logger.isDebugEnabled()) {
						logger.debug("Unknown line type: " + type);
					}
			}
		} catch (JsonProcessingException e) {
			logger.warn("Failed to parse JSON line", e);
		}
	}

	private void processSummary(JsonNode node) {
		if (summaries.isEmpty()) {
			System.err.println("[DevOutput] First summary fields: " + node.toString().substring(0, Math.min(500, node.toString().length())));
		}
		summaries.add(node);
	}

	private void processEvent(JsonNode node) {

		// Extract template hash + body (builds template map from flowing objects)
		String hash = node.path("templateHash").asText("");
		String template = node.path("template").asText("");

		if (!hash.isEmpty() && !template.isEmpty()) {
			templatesByHash.putIfAbsent(hash, template);
		}

		// Extract encoded form — only keep counters + bounded VR samples
		String encoded = node.path("encoded").asText("");
		if (!encoded.isEmpty()) {
			int encBytes = encoded.length() + 1; // ASCII-safe for encoded events
			totalEncodedBytes += encBytes;

			if (!hash.isEmpty()) {
				eventCountsByHash.merge(hash, 1, Integer::sum);
				encSizesByHash.merge(hash, (long) encBytes, Long::sum);

				// Keep only a few samples per hash for VR proof
				int maxSamples = options.devOutputVrEventsPerTemplate;
				List<String> samples = vrSamplesByHash.get(hash);
				if (samples == null) {
					samples = new ArrayList<>(maxSamples);
					vrSamplesByHash.put(hash, samples);
				}
				if (samples.size() < maxSamples) {
					samples.add(encoded);
				}
			}
		}
	}

	// --- Close: build and emit output ---

	@Override
	public void close() throws IOException {

		if (summaries.isEmpty()) {
			if (logger.isDebugEnabled()) {
				logger.debug("No summary data received, skipping dev output");
			}
			return;
		}

		try {
			ObjectNode payload = buildPayload();
			String jsonStr = mapper.writeValueAsString(payload);

			// Write to file if path specified
			if (options.devOutputFilePath != null && !options.devOutputFilePath.isBlank()) {
				try (FileOutputStream fos = new FileOutputStream(options.devOutputFilePath)) {
					fos.write(mapper.writerWithDefaultPrettyPrinter()
						.writeValueAsBytes(payload));
				}
			}

			if (options.devOutputLocalOnly) {
				printLocalOutput(payload, jsonStr);
			} else {
				String url = buildUrl(jsonStr);
				printUrlOutput(url, jsonStr);

				if (options.devOutputOpenBrowser) {
					openBrowser(url);
				}
			}

		} catch (Exception e) {
			logger.error("Failed to build dev output", e);
		}
	}

	// --- Payload construction ---

	private ObjectNode buildPayload() {

		long totalInputBytes = 0;
		long totalVolume = 0;

		// Collect all summaries and compute totals
		for (JsonNode s : summaries) {
			totalInputBytes += s.path("summaryBytes").asLong(0);
			totalVolume += s.path("summaryVolume").asLong(0);
		}

		double compressionPct = totalInputBytes > 0
			? Math.round((1.0 - ((double) totalEncodedBytes / totalInputBytes)) * 1000.0) / 10.0
			: 0;

		ObjectNode payload = mapper.createObjectNode();
		payload.put("v", 2);
		payload.put("in", totalInputBytes);
		payload.put("enc", totalEncodedBytes);
		payload.put("cmp", compressionPct);
		payload.put("ev", totalVolume);
		payload.put("pat", summaries.size());
		payload.put("tpl", templatesByHash.size());

		if (options.devOutputSourceName != null && !options.devOutputSourceName.isBlank()) {
			payload.put("src", options.devOutputSourceName);
		}

		if (options.devOutputDailyGb > 0) {
			payload.put("dgb", options.devOutputDailyGb);
		}

		// Top patterns by bytes
		buildTopPatterns(payload, totalInputBytes);

		// Distributions by enrichment type
		buildDistributions(payload, totalInputBytes);

		// Volume reduction samples
		buildVrSamples(payload);

		return payload;
	}

	private void buildTopPatterns(ObjectNode payload, long totalInputBytes) {

		int limit = options.devOutputTopPatterns;

		// Sort summaries by summaryBytes descending
		List<JsonNode> sorted = new ArrayList<>(summaries);
		sorted.sort((a, b) -> Long.compare(
			b.path("summaryBytes").asLong(0),
			a.path("summaryBytes").asLong(0)));

		ArrayNode topArray = mapper.createArrayNode();

		for (int i = 0; i < Math.min(limit, sorted.size()); i++) {

			JsonNode s = sorted.get(i);
			long bytes = s.path("summaryBytes").asLong(0);
			double pct = totalInputBytes > 0
				? Math.round((double) bytes / totalInputBytes * 10000.0) / 100.0
				: 0;

			ObjectNode pattern = mapper.createObjectNode();

			// Message pattern (first enrichment field, typically message_pattern)
			String messagePattern = extractField(s, "message_pattern");
			pattern.put("m", truncate(messagePattern, 200));
			pattern.put("p", pct);
			pattern.put("b", bytes);

			// Service
			String service = extractField(s, "tenx_user_service");
			if (!service.isEmpty()) {
				pattern.put("s", truncate(service, 40));
			}

			// Dimensions (enrichment values)
			ObjectNode dims = buildDimensions(s);
			if (dims.size() > 0) {
				pattern.set("d", dims);
			}

			topArray.add(pattern);
		}

		payload.set("top", topArray);
	}

	private void buildDistributions(ObjectNode payload, long totalInputBytes) {

		// Detect enrichment fields (fields that aren't summaryVolume/summaryBytes/standard)
		Set<String> standardFields = Set.of(
			"type", "message_pattern", "tenx_user_service",
			"summaryVolume", "summaryBytes", "summaryTotals");

		Map<String, String> enrichmentTypes = new LinkedHashMap<>();

		if (!summaries.isEmpty()) {
			summaries.get(0).fieldNames().forEachRemaining(field -> {
				if (!standardFields.contains(field)) {
					enrichmentTypes.put(field, mapEnrichmentType(field));
				}
			});
		}

		if (enrichmentTypes.isEmpty()) {
			return;
		}

		ObjectNode distNode = mapper.createObjectNode();

		for (Map.Entry<String, String> entry : enrichmentTypes.entrySet()) {

			String csvCol = entry.getKey();
			String etype = entry.getValue();

			// Aggregate by enrichment value
			Map<String, long[]> agg = new LinkedHashMap<>();

			for (JsonNode s : summaries) {
				String value = extractField(s, csvCol).trim();
				if (value.isEmpty()) continue;

				String truncated = truncateEnrichmentValue(value, etype);
				agg.computeIfAbsent(truncated, k -> new long[2]);
				agg.get(truncated)[0] += s.path("summaryBytes").asLong(0);
				agg.get(truncated)[1] += s.path("summaryVolume").asLong(0);
			}

			if (agg.isEmpty()) continue;

			// Sort by bytes descending
			List<Map.Entry<String, long[]>> sortedEntries = new ArrayList<>(agg.entrySet());
			sortedEntries.sort((a, b) -> Long.compare(b.getValue()[0], a.getValue()[0]));

			ArrayNode distArray = mapper.createArrayNode();
			for (Map.Entry<String, long[]> e : sortedEntries) {
				double pct = totalInputBytes > 0
					? Math.round((double) e.getValue()[0] / totalInputBytes * 1000.0) / 10.0
					: 0;
				ObjectNode item = mapper.createObjectNode();
				item.put("v", e.getKey());
				item.put("p", pct);
				distArray.add(item);
			}

			distNode.set(etype, distArray);
		}

		if (distNode.size() > 0) {
			payload.set("dist", distNode);
		}
	}

	private void buildVrSamples(ObjectNode payload) {

		if (templatesByHash.isEmpty() || vrSamplesByHash.isEmpty()) {
			return;
		}

		int numTemplates = options.devOutputVrTemplates;

		// Score candidates by reduction ratio * frequency
		List<VrCandidate> candidates = new ArrayList<>();

		for (Map.Entry<String, String> tpl : templatesByHash.entrySet()) {

			String hash = tpl.getKey();
			int count = eventCountsByHash.getOrDefault(hash, 0);
			if (count < 2) continue;

			int tplLen = tpl.getValue().length();
			double avgEnc = (double) encSizesByHash.getOrDefault(hash, 0L) / count;
			double ratio = avgEnc > 0 ? tplLen / avgEnc : 0;

			candidates.add(new VrCandidate(hash, ratio, count, tplLen));
		}

		if (candidates.isEmpty()) {
			return;
		}

		// Sort by ratio * count descending (impressive + frequent)
		candidates.sort((a, b) -> Double.compare(b.ratio * b.count, a.ratio * a.count));
		if (candidates.size() > numTemplates) {
			candidates = candidates.subList(0, numTemplates);
		}

		// Build output from pre-collected samples
		StringBuilder templateLines = new StringBuilder();
		StringBuilder encodedOutput = new StringBuilder();
		long totalOrigVolume = 0;
		long totalEncVolume = 0;
		int totalEventCount = 0;

		for (VrCandidate c : candidates) {
			List<String> samples = vrSamplesByHash.getOrDefault(c.hash, Collections.emptyList());
			if (samples.isEmpty()) continue;

			String tplBody = templatesByHash.get(c.hash);
			if (templateLines.length() > 0) templateLines.append('\n');
			try {
				ObjectNode tplNode = mapper.createObjectNode();
				tplNode.put("templateHash", c.hash);
				tplNode.put("template", tplBody);
				templateLines.append(mapper.writeValueAsString(tplNode));
			} catch (JsonProcessingException e) {
				continue;
			}

			for (String event : samples) {
				if (encodedOutput.length() > 0) encodedOutput.append('\n');
				encodedOutput.append(event);
			}

			totalOrigVolume += (long) c.tplLen * c.count;
			totalEncVolume += encSizesByHash.getOrDefault(c.hash, 0L);
			totalEventCount += c.count;
		}

		if (encodedOutput.length() == 0) return;

		ObjectNode vr = mapper.createObjectNode();
		vr.put("templates", templateLines.toString());
		vr.put("encoded", encodedOutput.toString());
		vr.put("events", totalEventCount);
		vr.put("origBytes", totalOrigVolume);
		vr.put("encBytes", totalEncVolume);
		payload.set("vr", vr);
	}

	// --- URL encoding ---

	private String buildUrl(String jsonStr) throws IOException {

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		Deflater deflater = new Deflater(Deflater.BEST_COMPRESSION);

		try (DeflaterOutputStream dos = new DeflaterOutputStream(baos, deflater)) {
			dos.write(jsonStr.getBytes(StandardCharsets.UTF_8));
		} finally {
			deflater.end();
		}

		String encoded = Base64.getUrlEncoder().withoutPadding()
			.encodeToString(baos.toByteArray());

		String baseUrl = (options.devOutputBaseUrl != null && !options.devOutputBaseUrl.isBlank())
			? options.devOutputBaseUrl
			: CONSOLE_URL;

		return baseUrl + "?step=1&d=" + encoded;
	}

	// --- Console output ---

	private void printUrlOutput(String url, String jsonStr) {

		StringBuilder sb = new StringBuilder();
		sb.append("\n").append("=".repeat(60)).append('\n');
		sb.append("VIEW RESULTS\n");
		sb.append("=".repeat(60)).append('\n');
		sb.append('\n').append(url).append("\n\n");
		sb.append("  Data: ").append(jsonStr.length()).append(" bytes JSON -> ")
			.append(url.length()).append(" chars URL\n");

		if (url.length() > 2000) {
			sb.append("  WARNING: URL is long (").append(url.length())
				.append(" chars). May not work in all browsers.\n");
		}

		appendSummaryStats(sb);
		System.out.println(sb);
	}

	private void printLocalOutput(ObjectNode payload, String jsonStr) {

		StringBuilder sb = new StringBuilder();
		sb.append("\n").append("=".repeat(60)).append('\n');
		sb.append("LOCAL MODE - NO DATA SENT EXTERNALLY\n");
		sb.append("=".repeat(60)).append('\n');
		sb.append("  Data size: ").append(jsonStr.length()).append(" bytes JSON\n");
		sb.append("  To view: copy JSON and use 'Paste Results' at https://console.log10x.com?step=1\n");

		appendSummaryStats(sb);

		sb.append("\n").append("=".repeat(60)).append('\n');
		sb.append("JSON DATA\n");
		sb.append("=".repeat(60)).append('\n');
		try {
			sb.append(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(payload));
		} catch (JsonProcessingException e) {
			sb.append(jsonStr);
		}

		System.out.println(sb);
	}

	private void appendSummaryStats(StringBuilder sb) {

		long totalBytes = 0;
		long totalVolume = 0;
		for (JsonNode s : summaries) {
			totalBytes += s.path("summaryBytes").asLong(0);
			totalVolume += s.path("summaryVolume").asLong(0);
		}

		double compressionPct = totalBytes > 0
			? Math.round((1.0 - ((double) totalEncodedBytes / totalBytes)) * 1000.0) / 10.0
			: 0;

		sb.append("\n").append("=".repeat(60)).append('\n');
		sb.append("ANALYSIS SUMMARY\n");
		sb.append("=".repeat(60)).append('\n');
		sb.append("  Input size:      ").append(formatBytes(totalBytes)).append('\n');
		sb.append("  Encoded size:    ").append(formatBytes(totalEncodedBytes)).append('\n');
		sb.append("  Compression:     ").append(compressionPct).append("%\n");
		sb.append("  Total events:    ").append(String.format("%,d", totalVolume)).append('\n');
		sb.append("  Unique patterns: ").append(String.format("%,d", summaries.size())).append('\n');
		sb.append("  Templates:       ").append(String.format("%,d", templatesByHash.size())).append('\n');
	}

	private static void openBrowser(String url) {
		try {
			String os = System.getProperty("os.name", "").toLowerCase();
			ProcessBuilder pb;
			if (os.contains("mac")) {
				pb = new ProcessBuilder("open", url);
			} else if (os.contains("win")) {
				pb = new ProcessBuilder("cmd", "/c", "start", url);
			} else {
				pb = new ProcessBuilder("xdg-open", url);
			}
			pb.start();
		} catch (IOException e) {
			logger.debug("Failed to open browser", e);
		}
	}

	// --- Helpers ---

	/**
	 * Extract a field value from a summary node.
	 * Enrichment fields may arrive as single-element arrays (e.g., ["value"])
	 * or as plain strings.
	 */
	private static String extractField(JsonNode node, String fieldName) {
		JsonNode field = node.path(fieldName);
		if (field.isArray() && field.size() > 0) {
			return field.get(0).asText("");
		}
		return field.asText("");
	}

	private ObjectNode buildDimensions(JsonNode summary) {

		Set<String> skip = Set.of(
			"type", "message_pattern", "tenx_user_service",
			"summaryVolume", "summaryBytes", "summaryTotals");

		ObjectNode dims = mapper.createObjectNode();

		summary.fieldNames().forEachRemaining(field -> {
			if (!skip.contains(field)) {
				String value = extractField(summary, field).trim();
				if (!value.isEmpty()) {
					String etype = mapEnrichmentType(field);
					dims.put(etype, truncateEnrichmentValue(value, etype));
				}
			}
		});

		return dims;
	}

	private static String mapEnrichmentType(String csvCol) {
		switch (csvCol) {
			case "severity_level": return "level";
			case "http_status_class":
			case "http_code": return "http";
			case "log_group": return "group";
			case "geo_country":
			case "geo_region": return "geo";
			default: return csvCol.replace("_", "");
		}
	}

	private static String truncateEnrichmentValue(String value, String etype) {
		switch (etype) {
			case "level":
				String v = (value == null || value.isEmpty()) ? "UNCLASSIFIED" : value;
				return v.substring(0, Math.min(4, v.length())).toUpperCase();
			case "http":
				return value.substring(0, Math.min(3, value.length()));
			default:
				return value.substring(0, Math.min(10, value.length()));
		}
	}

	private static String truncate(String s, int maxLen) {
		return s.length() <= maxLen ? s : s.substring(0, maxLen);
	}

	private static String formatBytes(long b) {
		if (b >= 1_000_000_000) return String.format("%.1fGB", b / 1_000_000_000.0);
		if (b >= 1_000_000) return String.format("%.1fMB", b / 1_000_000.0);
		if (b >= 1_000) return String.format("%.1fKB", b / 1_000.0);
		return b + "B";
	}

	// --- Inner types ---

	private static class VrCandidate {
		final String hash;
		final double ratio;
		final int count;
		final int tplLen;

		VrCandidate(String hash, double ratio, int count, int tplLen) {
			this.hash = hash;
			this.ratio = ratio;
			this.count = count;
			this.tplLen = tplLen;
		}
	}
}
