import os

input_path = r"input/show.csv"
output_path = r"output/show_fixed.csv"

os.makedirs(os.path.dirname(output_path), exist_ok=True)

with open(input_path, "r", encoding="utf-8", newline="") as f_in, \
     open(output_path, "w", encoding="utf-8", newline="") as f_out:
    
    fixed_lines = []

    for line in f_in:
        # Strip only the trailing newline for safe concatenation
        stripped = line.rstrip("\n")

        if stripped.startswith('"') and fixed_lines:
            # Attach this line to the previous one
            # (you can add a space in between if needed)
            fixed_lines[-1] = fixed_lines[-1].rstrip("\n") + stripped + "\n"
        else:
            # Keep the line as is
            fixed_lines.append(stripped + "\n")

    # Write all fixed lines
    f_out.writelines(fixed_lines)

print(f"Fixed file written to: {output_path}")