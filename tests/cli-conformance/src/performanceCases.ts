import type { FixtureAction } from "./types.ts";

export type PerformanceCase = {
  id: string;
  description: string;
  args: string[];
  setup?: FixtureAction[];
  margin?: number;
};

const TEN_MESSAGES = "{dataDir}/TenMessages/TenMessages-ch-chx-mx-pad-rch-rsh-st-sum.mcap";

export const performanceCases: PerformanceCase[] = [
  {
    id: "info-ten-messages",
    description: "Read summary information from an indexed MCAP.",
    args: ["info", TEN_MESSAGES],
  },
  {
    id: "cat-ten-messages",
    description: "Print message previews for an indexed MCAP.",
    args: ["cat", TEN_MESSAGES],
  },
  {
    id: "filter-ten-messages",
    description: "Filter messages by topic and write an MCAP output.",
    args: ["filter", TEN_MESSAGES, "-o", "{caseWorkDir}/filtered.mcap", "-y", "example"],
  },
  {
    id: "compress-ten-messages",
    description: "Rewrite an MCAP with default compression.",
    args: ["compress", TEN_MESSAGES, "-o", "{caseWorkDir}/compressed.mcap"],
  },
];
