#!/bin/bash
fastqc SRR33637628.fastq.gz
minimap2 -a NC_000913.3.fasta SRR33637628.fastq > alignments.sam
samtools faidx NC_000913.3.fasta > NC_000913.3.fasta.fai
samtools view -bo aln.bam alignments.sam
samtools flagstat aln.bam | grep -E 'mapped \(' | awk -F '[()%]' '{print $2}' > stats.txt
file=stats.txt
first_line=$(head -n 1 "$file")
value=$(echo "$first_line" | grep -oE '[0-9]+' | head -n 1)
if [ "$value" -gt 80 ]; then
    echo "OK">>stats.txt
    samtools sort aln.bam > alns.bam
    freebayes -f NC_000913.3.fasta alns.bam>vc.vcf
    echo "Finished!"
else
    echo "NOT OK">>stats.txt
fi
