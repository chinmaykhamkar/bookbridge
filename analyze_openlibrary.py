#!/usr/bin/env python3
"""
OpenLibrary Data Analysis Script
Analyzes the first 100K lines to understand data distribution and quality
"""

import json
import re
from collections import defaultdict
import os

def analyze_openlibrary_dump(file_path, sample_size=1000000):
    """Analyze OpenLibrary dump file structure and content"""
    
    print(f"Analyzing first {sample_size:,} lines from: {file_path}")
    print("-" * 60)
    
    # Statistics tracking
    stats = {
        'total_lines': 0,
        'valid_lines': 0,
        'invalid_lines': 0,
        'type_counts': defaultdict(int),
        'sample_records': {},
        'errors': defaultdict(int)
    }
    
    try:
        file_size = os.path.getsize(file_path)
        print(f"File size: {file_size / (1024**3):.1f} GB")
        print()
    except:
        print("Could not determine file size")
    
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
        for line_num in range(sample_size):
            try:
                line = file.readline()
                if not line:  # End of file
                    break
                    
                line = line.strip()
                if not line:
                    continue
                
                stats['total_lines'] += 1
                
                # Parse the tab-separated format
                parts = line.split('\t')
                
                if len(parts) < 4:
                    stats['invalid_lines'] += 1
                    stats['errors']['insufficient_parts'] += 1
                    continue
                
                record_type = parts[0]
                key = parts[1]
                revision = parts[2]
                timestamp = parts[3]
                
                # JSON data might span multiple tab-separated parts
                json_data = '\t'.join(parts[4:]) if len(parts) > 4 else '{}'
                
                # Count record types
                stats['type_counts'][record_type] += 1
                stats['valid_lines'] += 1
                
                # Store sample records for each type (first occurrence only)
                if record_type not in stats['sample_records']:
                    try:
                        parsed_json = json.loads(json_data)
                        stats['sample_records'][record_type] = {
                            'key': key,
                            'revision': revision,
                            'timestamp': timestamp,
                            'data_sample': parsed_json
                        }
                    except json.JSONDecodeError:
                        stats['errors']['json_decode'] += 1
                
                # Progress indicator
                if line_num > 0 and line_num % 10000 == 0:
                    print(f"Processed {line_num:,} lines...")
                    
            except Exception as e:
                stats['errors'][f'parse_error_{type(e).__name__}'] += 1
                continue
    
    # Print analysis results
    print_analysis_results(stats)
    return stats

def print_analysis_results(stats):
    """Print formatted analysis results"""
    
    print(f"\n{'='*60}")
    print("ANALYSIS RESULTS")
    print(f"{'='*60}")
    
    print(f"Total lines processed: {stats['total_lines']:,}")
    print(f"Valid lines: {stats['valid_lines']:,}")
    print(f"Invalid lines: {stats['invalid_lines']:,}")
    print()
    
    print("RECORD TYPE DISTRIBUTION:")
    print("-" * 40)
    
    # Sort by count (descending)
    sorted_types = sorted(stats['type_counts'].items(), key=lambda x: x[1], reverse=True)
    
    for record_type, count in sorted_types:
        percentage = (count / stats['valid_lines']) * 100 if stats['valid_lines'] > 0 else 0
        print(f"{record_type:25} {count:8,} ({percentage:5.1f}%)")
    
    print()
    print("RELEVANT RECORD TYPES FOR BOOKBRIDGE:")
    print("-" * 40)
    
    relevant_types = ['/type/work', '/type/edition', '/type/author']
    total_relevant = 0
    
    for record_type in relevant_types:
        count = stats['type_counts'].get(record_type, 0)
        total_relevant += count
        percentage = (count / stats['valid_lines']) * 100 if stats['valid_lines'] > 0 else 0
        print(f"{record_type:25} {count:8,} ({percentage:5.1f}%)")
    
    if total_relevant > 0:
        relevant_percentage = (total_relevant / stats['valid_lines']) * 100
        print(f"{'TOTAL RELEVANT':25} {total_relevant:8,} ({relevant_percentage:5.1f}%)")
    
    print()
    print("SAMPLE RECORD STRUCTURES:")
    print("-" * 40)
    
    for record_type in relevant_types:
        if record_type in stats['sample_records']:
            print(f"\n{record_type}:")
            sample = stats['sample_records'][record_type]
            print(f"  Key: {sample['key']}")
            print(f"  Timestamp: {sample['timestamp']}")
            
            # Print key fields from JSON data
            data = sample['data_sample']
            if record_type == '/type/work':
                print_work_sample(data)
            elif record_type == '/type/edition':
                print_edition_sample(data)
            elif record_type == '/type/author':
                print_author_sample(data)
    
    if stats['errors']:
        print(f"\nERRORS ENCOUNTERED:")
        print("-" * 40)
        for error_type, count in stats['errors'].items():
            print(f"{error_type:25} {count:8,}")

def print_work_sample(data):
    """Print sample work record structure"""
    print(f"    Title: {data.get('title', 'N/A')}")
    print(f"    Authors: {len(data.get('authors', []))} author(s)")
    print(f"    Subjects: {len(data.get('subjects', []))} subject(s)")
    print(f"    Covers: {len(data.get('covers', []))} cover(s)")
    
    if 'description' in data:
        desc = data['description']
        if isinstance(desc, dict):
            desc_text = desc.get('value', '')
        else:
            desc_text = str(desc)
        print(f"    Description: {len(desc_text)} characters")

def print_edition_sample(data):
    """Print sample edition record structure"""
    print(f"    Title: {data.get('title', 'N/A')}")
    print(f"    ISBN-13: {data.get('isbn_13', [])}")
    print(f"    ISBN-10: {data.get('isbn_10', [])}")
    print(f"    Pages: {data.get('number_of_pages', 'N/A')}")
    print(f"    Publish Date: {data.get('publish_date', 'N/A')}")
    print(f"    Publishers: {data.get('publishers', [])}")
    print(f"    Languages: {data.get('languages', [])}")

def print_author_sample(data):
    """Print sample author record structure"""
    print(f"    Name: {data.get('name', 'N/A')}")
    print(f"    Birth Date: {data.get('birth_date', 'N/A')}")
    print(f"    Death Date: {data.get('death_date', 'N/A')}")
    
    bio = data.get('bio', {})
    if isinstance(bio, dict):
        bio_text = bio.get('value', '')
    else:
        bio_text = str(bio) if bio else ''
    print(f"    Bio: {len(bio_text)} characters")

def estimate_full_dataset(stats, file_path):
    """Estimate full dataset statistics"""
    
    print(f"\n{'='*60}")
    print("FULL DATASET ESTIMATES")
    print(f"{'='*60}")
    
    try:
        file_size = os.path.getsize(file_path)
        sample_size = stats['total_lines']
        
        # Estimate total lines in full file
        estimated_total_lines = (file_size / sample_size) * (sample_size / (87 * 1024**3)) * sample_size
        
        print(f"Estimated total lines in 87GB file: {estimated_total_lines:,.0f}")
        
        # Estimate relevant records
        relevant_types = ['/type/work', '/type/edition', '/type/author']
        for record_type in relevant_types:
            sample_count = stats['type_counts'].get(record_type, 0)
            if sample_count > 0:
                estimated_count = (sample_count / sample_size) * estimated_total_lines
                print(f"Estimated {record_type}: {estimated_count:,.0f}")
                
    except Exception as e:
        print(f"Could not estimate full dataset: {e}")

if __name__ == "__main__":
    # Update this path to your OpenLibrary dump file
    dump_file_path = "../../../Downloads/ol_dump_2025-07-31.txt"  # Change this to your actual file path
    
    # Check if file exists
    if not os.path.exists(dump_file_path):
        print(f"Error: File '{dump_file_path}' not found!")
        print("Please update the 'dump_file_path' variable with the correct path to your OpenLibrary dump file.")
        exit(1)
    
    # Run analysis
    results = analyze_openlibrary_dump(dump_file_path, sample_size=114135941)
    
    # Estimate full dataset
    estimate_full_dataset(results, dump_file_path)
    
    print(f"\n{'='*60}")
    print("NEXT STEPS:")
    print("1. Review the record type distribution above")
    print("2. Check the sample record structures")
    print("3. Proceed with building the streaming processor")
    print(f"{'='*60}")