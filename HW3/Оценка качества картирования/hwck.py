from clearml import PipelineDecorator, StorageManager, Logger, Task
import subprocess
import os
import re
import logging

def check_dependencies():
    required_tools = ['samtools', 'minimap2', 'fastqc', 'freebayes']
    missing = []
    for tool in required_tools:
        if subprocess.run(['which', tool], capture_output=True).returncode != 0:
            missing.append(tool)
    if missing:
        raise EnvironmentError(f"Missing required tools: {', '.join(missing)}")

@PipelineDecorator.component(return_values=['qc_status'], cache=True)
def fastqc_check(fastq_path: str, output_txt: str):
    try:
        fastq_path = os.path.abspath(fastq_path)
        output_txt = os.path.abspath(output_txt)
        
        result = subprocess.run(
            ['fastqc', fastq_path, '--extract', '-o', os.path.dirname(output_txt)],
            capture_output=True,
            text=True
        )
        result.check_returncode()
        
        if not os.path.exists(output_txt):
            raise FileNotFoundError(f"FastQC output not generated: {output_txt}")
            
        return "PASS" if "FAIL" not in open(output_txt).read() else "FAIL"

    except Exception as e:
        Logger.current_logger().report_text(f"FastQC failed: {str(e)}", level=logging.ERROR)
        raise

@PipelineDecorator.component(cache=True)
def alignment(fasta_path: str, fastq_path: str, sam_output: str):
    try:
        sam_output = os.path.abspath(sam_output)
        
        Logger.current_logger().report_text("Starting alignment with minimap2")
        result = subprocess.run(
            ['minimap2', '-ax', 'sr', os.path.abspath(fasta_path), os.path.abspath(fastq_path)],
            stdout=open(sam_output, 'w'),
            stderr=subprocess.PIPE,
            text=True
        )
        result.check_returncode()
        
        if os.path.getsize(sam_output) == 0:
            raise ValueError("Empty SAM file generated")
        Logger.current_logger().report_text(f"Alignment completed, SAM size: {os.path.getsize(sam_output)} bytes")

    except Exception as e:
        Logger.current_logger().report_text(f"Alignment failed: {str(e)}\nSTDERR: {result.stderr}", level=logging.ERROR)
        raise

@PipelineDecorator.component(cache=True)
def sam_to_bam(sam_path: str, bam_output: str):
    try:
        sam_path = os.path.abspath(sam_path)
        bam_output = os.path.abspath(bam_output)
        
        Logger.current_logger().report_text("Converting SAM to BAM")
        result = subprocess.run(
            ['samtools', 'view', '-b', sam_path, '-o', bam_output],
            capture_output=True,
            text=True
        )
        result.check_returncode()
        
        if os.path.getsize(bam_output) == 0:
            raise ValueError("Empty BAM file generated")
        Logger.current_logger().report_text(f"Conversion successful, BAM size: {os.path.getsize(bam_output)} bytes")

    except Exception as e:
        err_msg = f"SAM to BAM failed: {str(e)}"
        if result.stderr:
            err_msg += f"\nSTDERR: {result.stderr}"
        Logger.current_logger().report_text(err_msg, level=logging.ERROR)
        raise

@PipelineDecorator.component(return_values=['mapped_percent'], cache=True)
def flagstat(bam_path: str, stats_output: str):
    try:
        bam_path = os.path.abspath(bam_path)
        stats_output = os.path.abspath(stats_output)
        
        if not os.path.exists(bam_path):
            raise FileNotFoundError(f"BAM file missing: {bam_path}")
        if os.path.getsize(bam_path) == 0:
            raise ValueError("Empty BAM file provided")

        Logger.current_logger().report_text("Running samtools flagstat")
        result = subprocess.run(
            ['samtools', 'flagstat', bam_path],
            capture_output=True,
            text=True,
            check=True
        )
        
        with open(stats_output, 'w') as f:
            f.write(result.stdout)
        
        mapped_line = [line for line in result.stdout.split('\n') if 'mapped (' in line][0]
        mapped_percent = float(re.search(r'(\d+\.\d+)%', mapped_line).group(1))
        
        # Fixed report_scalar call with iteration parameter
        Logger.current_logger().report_scalar(
            title="Alignment Quality",
            series="Mapped Reads",
            value=mapped_percent,
            iteration=0  # Added required iteration parameter
        )
        return mapped_percent

    except Exception as e:
        Logger.current_logger().report_text(f"Flagstat failed: {str(e)}", level=logging.ERROR)
        raise

@PipelineDecorator.component(cache=True)
def sort_and_variant_calling(bam_path: str, fasta_path: str, vcf_output: str):
    try:
        sorted_bam = bam_path.replace('.bam', '.sorted.bam')
        Logger.current_logger().report_text("Sorting BAM file")
        subprocess.run(
            ['samtools', 'sort', bam_path, '-o', sorted_bam],
            check=True
        )
        
        Logger.current_logger().report_text("Running FreeBayes variant calling")
        with open(vcf_output, 'w') as f:
            subprocess.run(
                ['freebayes', '-f', fasta_path, sorted_bam],
                stdout=f,
                check=True
            )

    except Exception as e:
        Logger.current_logger().report_text(f"Variant calling failed: {str(e)}", level=logging.ERROR)
        raise

@PipelineDecorator.pipeline(
    name='Genome Alignment Pipeline',
    project='Genomics Workflows',
    version='1.5'
)
def genome_pipeline(fasta_path: str, fastq_path: str):
    check_dependencies()
    
    qc_status = fastqc_check(fastq_path, 'fastqc_report.txt')
    
    sam_file = 'alignment.sam'
    alignment(fasta_path, fastq_path, sam_file)
    
    bam_file = 'alignment.bam'
    sam_to_bam(sam_file, bam_file)
    
    stats_file = 'mapping_stats.txt'
    mapped_percent = flagstat(bam_file, stats_file)
    
    if mapped_percent >= 90.0:
        sort_and_variant_calling(bam_file, fasta_path, 'variants.vcf')
    else:
        Logger.current_logger().report_text(
            f"Rejecting alignment with {mapped_percent}% mapped reads",
            level=logging.ERROR
        )
    task = Task.current_task()
    task.set_parameter("General/demo_image", "https://example.com/genomics-pipeline-flow.png")
    task.add_tags(["genomics", "alignment", "variant-calling"])

if __name__ == '__main__':
    # Initialize ClearML task first with proper error handling
    try:
        task = Task.init(project_name='Genomics Workflows', task_name='Genome Alignment Pipeline')
        PipelineDecorator.run_locally()
        
        # Get input files
        fasta = os.path.abspath(StorageManager.get_local_copy('./NC_000913.3.fasta'))
        fastq = os.path.abspath(StorageManager.get_local_copy('./SRR33637628.fastq'))
        
        # Validate inputs
        if not all(os.path.exists(f) for f in [fasta, fastq]):
            raise FileNotFoundError("Missing input files")
        if os.path.getsize(fasta) == 0 or os.path.getsize(fastq) == 0:
            raise ValueError("Empty input files detected")
            
        genome_pipeline(fasta, fastq)
        
    except Exception as e:
        # Ensure logger is initialized before use
        if Logger.current_logger():
            Logger.current_logger().report_text(
                f"Pipeline failed: {str(e)}",
                level=logging.ERROR
            )
        else:
            print(f"Critical error: {str(e)}")
        raise
