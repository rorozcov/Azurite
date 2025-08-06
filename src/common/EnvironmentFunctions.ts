export function parseBlobVersioning(flags: {
  [key: string]: any;
}): boolean | undefined {
  const value = flags?.blobVersioning;

  if (value === undefined) {
    // If not specified, return undefined
    return undefined;
  }

  // If already boolean, return it
  if (typeof value === "boolean") {
    return value;
  }

  // Handle string representations
  if (typeof value === "string") {
    const lowercased = value.toLowerCase();

    if (lowercased === "true") {
      return true;
    }

    if (lowercased === "false") {
      return false;
    }

    throw new Error(
      `Invalid blobVersioning value: ${value}. Must be true or false.`
    );
  }

  throw new Error("blobVersioning must be a boolean value (true or false)");
}
