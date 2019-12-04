# Merluvlee
# Lisa Malins

configfile: "config.yaml"

rule targets:
    input:
        expand("data/kmer-counts/{name}_{k}mer_histo.txt", name=config["reads"], k=config["mer_size"]),
        expand("data/sql/combined_{names}_{k}mer_counts.csv", names="_".join(config["reads"]), k=config["mer_size"])

def constrain(arg):
    if type(arg) == list:
        return "|".join(arg)
    else:
        return arg

wildcard_constraints:
    name=constrain(list(config["reads"].keys())),
    fastq=constrain([config["reads"][set]["fastq"].rstrip(".gz") for set in config["reads"]])


def get_fastq(wildcards):
    for set in config["reads"]:
        if wildcards.name == config["reads"][set]["name"]:
            return "data/reads/" + config["reads"][set]["fastq"].rstrip(".gz")

def __expected_kmers(name, k):
    G = config["reads"][name]["genome_size"]
    c = config["reads"][name]["coverage"]
    e = config["error_rate"]
    k = k
    num_megabases = str(round(G + G * c * e * k))
    return num_megabases + "M"

def expected_kmers(wildcards):
    return __expected_kmers(wildcards.name, int(wildcards.k))

rule gunzip:
    input:
        "data/reads/{fastq}.gz"
    output:
        "data/reads/{fastq}"
    threads:
        16
    shell:
        "unpigz -p {threads} -c {input} > {output}"

rule count_pass1:
    input:
        get_fastq
    params:
        bcsize=expected_kmers
    output:
        temp("data/kmer-counts/{name}_{k}.bc")
    threads: 16
    shell:
        "jellyfish bc -m {wildcards.k} -C -s {params.bcsize} -t {threads} -o {output} {input}"

rule count_pass2:
    input:
        fastq=get_fastq,
        bc="data/kmer-counts/{name}_{k}.bc"
    params:
        genomesize=lambda wildcards: str(config["reads"][wildcards.name]["genome_size"]) + "M"
    output:
        "data/kmer-counts/{name}_{k}mer_counts.jf"
    threads: 16
    shell:
        "jellyfish count -m {wildcards.k} -C -s {params.genomesize} -t {threads} --bc {input.bc} -o {output} {input.fastq}"

rule jellyfish_dump:
    input:
        "data/kmer-counts/{name}_{k}mer_counts.jf"
    output:
        "data/kmer-counts/{name}_{k}mer_dumps.fa"
    shell:
        "jellyfish dump {input} > {output}"

rule jellyfish_histo:
    input:
        "data/kmer-counts/{name}_{k}mer_counts.jf"
    output:
        "data/kmer-counts/{name}_{k}mer_histo.txt"
    shell:
        "jellyfish histo {input} > {output}"

rule print_jelly_size:
    run:
        for set in config["reads"]:
            total_kmers = __expected_kmers(set, config["mer_size"])
            memory = round(int(total_kmers.rstrip("MG")) * 18 / 8)
            print(set, total_kmers, "k-mers expected")
            print(set, str(config["reads"][set]["genome_size"]) + "M", "k-mers expected more than once")
            print(set, "expected memory size", memory, "Megabytes" )

# Split Jellyfish dump files by first 2 nucleotides.
# Grab sequences and preceding count then remove group separators
rule split_dump:
    input:
        "data/kmer-counts/{name}_{k}mer_dumps.fa"
    output:
        "data/kmer-counts/{name}_{k}mer_dumps_{nn}.fa"
    shell:
        """
        grep "^{wildcards.nn}" {input} -B 1 | grep -v "^--" > {output}
        """

rule build_tables:
    input:
        "data/kmer-counts/{name}_{k}mer_dumps_{nn}.fa"
    output:
        "data/sql/{name}_{k}mers_{nn}.db"
    shell:
        "python Scripts/KmerDatabase.py {input} {output}"

rule join_tables:
    input:
        expand("data/sql/{name}_{{k}}mers_{{nn}}.db", name=["{name1}", "{name2}"])
    output:
        "data/sql/combined_{name1}_{name2}_{k}mers_{nn}.db"
    shell:
        "python Scripts/JoinKmerDatabase.py {input} {output}"

rule unite_tables:
    input:
        expand("data/sql/combined_{{name1}}_{{name2}}_{{k}}mers_{n1}{n2}.db", \
        n1 = ["A", "C", "T", "G"], n2 = ["A", "C", "T", "G"])
    output:
        "data/sql/combined_{name1}_{name2}_{k}mers.db"
    run:
        pass

rule dump_counts:
    input:
        "data/sql/combined_{name1}_{name2}_{k}mers_{nn}.db"
    output:
        "data/sql/combined_{name1}_{name2}_{k}mer_counts_{nn}.csv"
    shell:
        "python Scripts/DumpCounts.py {input} {output}"

rule concatenate_counts:
    input:
        expand("data/sql/combined_{{name1}}_{{name2}}_{{k}}mer_counts_{n1}{n2}.csv", \
        n1 = ["A", "C", "T", "G"], n2 = ["A", "C", "T", "G"])
    output:
        "data/sql/combined_{name1}_{name2}_{k}mer_counts.csv"
    shell:
        "cat {input} > {output}"

# Remove low-hanging fruit intermediate files
# Ignore bash errors of "No such file or directory"
rule sweep:
    shell:
        """
        set +e
        rm data/reads/*.fastq data/kmer-counts/*.bc
        exit 0
        """
