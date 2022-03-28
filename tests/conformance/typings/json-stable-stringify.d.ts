declare module "json-stable-stringify" {
  export default function stringify(
    obj: unknown,
    opts?: {
      cmp?: (_: { key: string; value: unknown }, _: { key: string; value: unknown }) => number;
      space?: string | number;
      replacer?: string[] | ((key: string, value: unknown) => unknown);
    },
  ): string;
}
