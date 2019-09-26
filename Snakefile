# Merluvlee
# Lisa Malins

configfile: "config.yaml"

rule targets:
    input:
        expand("data/kmer-counts/{name}_{k}mer_histo.txt", name=config["reads"], k=config["mer_size"])

def get_fastq(wildcards):
    for set in config["reads"]:
        if wildcards.name == config["reads"][set]["name"]:
            return "data/reads/" + config["reads"][set]["fastq"].rstrip(".gz")

def __expected_kmers(name, k):
    G = config["genome_size"]
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
        temp("data/reads/{fastq}")
    shell:
        "gunzip -c {input} > {output}"

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
        genomesize=str(config["genome_size"]) + "M",
    output:
        temp("data/kmer-counts/{name}_{k}mer_counts.jf")
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
            print(set, str(config["genome_size"]) + "M", "k-mers expected more than once")
            print(set, "expected memory size", memory, "Megabytes" )
