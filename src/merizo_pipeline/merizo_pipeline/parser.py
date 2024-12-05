import csv
import json
from collections import defaultdict
import statistics

def parse_results(id, output_dir):
    cath_ids = defaultdict(int)
    plDDT_values = []
    input_file = f"{output_dir}/{id}_search.tsv"
    with open(input_file, "r") as fhIn:
        next(fhIn)
        msreader = csv.reader(fhIn, delimiter='\t')
        for row in msreader:
            plDDT_values.append(float(row[3]))
            meta = row[15]
            data = json.loads(meta)
            cath_ids[data["cath"]] += 1

    with open(f"{id}.parsed", "w", encoding="utf-8") as fhOut:
        mean_plddt = statistics.mean(plDDT_values) if plDDT_values else 0
        fhOut.write(f"#{id}_search.tsv Results. mean plddt: {mean_plddt}\n")
        fhOut.write("cath_id,count\n")
        for cath, v in cath_ids.items():
            fhOut.write(f"{cath},{v}\n")
