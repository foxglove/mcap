import fs from "node:fs/promises";
import path from "node:path";

export default async function* listDirRecursive(
  dirPath: string,
): AsyncGenerator<string, void, void> {
  for (const dirent of await fs.readdir(dirPath, { withFileTypes: true })) {
    if (dirent.isDirectory()) {
      for await (const subpath of listDirRecursive(path.join(dirPath, dirent.name))) {
        yield path.join(dirent.name, subpath);
      }
    } else {
      yield dirent.name;
    }
  }
}
