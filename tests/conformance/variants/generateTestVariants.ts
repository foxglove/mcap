import inputs from "./inputs";
import { TestFeatures, TestVariant } from "./types";

function* generateFeatureCombinations(
  ...features: TestFeatures[]
): Generator<Set<TestFeatures>, void, void> {
  if (features.length === 0) {
    yield new Set();
    return;
  }
  for (const variant of generateFeatureCombinations(...features.slice(1))) {
    yield variant;
    yield new Set([features[0]!, ...variant]);
  }
}

export default function* generateTestVariants(): Generator<TestVariant, void, void> {
  for (const input of inputs) {
    for (const features of generateFeatureCombinations(...Object.values(TestFeatures))) {
      // validate that variant features make sense for the data
      if (
        features.has(TestFeatures.UseAttachmentIndex) &&
        !input.records.some((record) => record.type === "Attachment")
      ) {
        continue;
      }
      if (
        features.has(TestFeatures.UseMetadataIndex) &&
        !input.records.some((record) => record.type === "Metadata")
      ) {
        continue;
      }
      if (
        features.has(TestFeatures.UseRepeatedSchemas) &&
        !input.records.some((record) => record.type === "Schema")
      ) {
        continue;
      }
      if (
        features.has(TestFeatures.UseRepeatedChannelInfos) &&
        !input.records.some((record) => record.type === "Channel")
      ) {
        continue;
      }
      if (
        !input.records.some(
          (record) =>
            record.type === "Message" || record.type === "Channel" || record.type === "Schema",
        ) &&
        (features.has(TestFeatures.UseChunks) ||
          features.has(TestFeatures.UseChunkIndex) ||
          features.has(TestFeatures.UseMessageIndex))
      ) {
        continue;
      }
      if (
        features.has(TestFeatures.UseSummaryOffset) &&
        !(
          features.has(TestFeatures.UseChunkIndex) ||
          features.has(TestFeatures.UseRepeatedSchemas) ||
          features.has(TestFeatures.UseRepeatedChannelInfos) ||
          features.has(TestFeatures.UseMetadataIndex) ||
          features.has(TestFeatures.UseAttachmentIndex) ||
          features.has(TestFeatures.UseStatistics)
        )
      ) {
        continue;
      }
      if (
        (features.has(TestFeatures.UseChunkIndex) || features.has(TestFeatures.UseMessageIndex)) &&
        !features.has(TestFeatures.UseChunks)
      ) {
        continue;
      }

      //FIXME: filter out message index without chunk

      const name = [input.baseName, ...Array.from(features).sort()].join("-");

      yield { ...input, name, features };
    }
  }
}
