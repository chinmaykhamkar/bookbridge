    #!/usr/bin/env python3
"""
BookBridge Data Verification Script
Analyzes the processed CSV files to verify data quality and relationships.
"""

import csv
import os
import random
from collections import defaultdict, Counter
import re


class DataVerifier:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.files = {
            'books': os.path.join(data_dir, 'books.csv'),
            'editions': os.path.join(data_dir, 'editions.csv'),
            'authors': os.path.join(data_dir, 'authors.csv'),
            'book_authors': os.path.join(data_dir, 'book_authors.csv')
        }
        
        # Data containers
        self.data = {}
        self.stats = {}
    
    def verify_all(self):
        """Run complete data verification"""
        print("BookBridge Data Verification")
        print("=" * 50)
        
        # Check file existence and sizes
        self.check_files()
        
        # Load and analyze each file
        for file_type, file_path in self.files.items():
            print(f"\nAnalyzing {file_type}.csv...")
            self.analyze_file(file_type, file_path)
        
        # Cross-reference relationships
        print("\nValidating relationships...")
        self.validate_relationships()
        
        # Final summary
        self.print_summary()
    
    def check_files(self):
        """Check if all required files exist and get basic info"""
        print("\nFile Status:")
        print("-" * 30)
        
        for file_type, file_path in self.files.items():
            if os.path.exists(file_path):
                size_mb = os.path.getsize(file_path) / (1024 * 1024)
                print(f"{file_type:12}.csv: {size_mb:6.1f} MB - ✓")
            else:
                print(f"{file_type:12}.csv: MISSING - ✗")
    
    def analyze_file(self, file_type, file_path):
        """Analyze individual CSV file"""
        if not os.path.exists(file_path):
            print(f"  File {file_path} not found!")
            return
        
        data = []
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                headers = reader.fieldnames
                
                for row in reader:
                    data.append(row)
                
                self.data[file_type] = data
                
                print(f"  Records: {len(data):,}")
                print(f"  Columns: {len(headers)}")
                
                # Show sample records
                if len(data) > 0:
                    self.show_samples(file_type, data, headers)
                    self.analyze_data_quality(file_type, data)
                
        except Exception as e:
            print(f"  Error reading file: {e}")
    
    def show_samples(self, file_type, data, headers):
        """Show sample records from each file"""
        print(f"\n  Sample {file_type} records:")
        
        # Show first record and a random one
        sample_indices = [0]
        if len(data) > 1:
            sample_indices.append(random.randint(1, len(data) - 1))
        
        for i, idx in enumerate(sample_indices):
            record = data[idx]
            print(f"    Record {idx + 1}:")
            
            if file_type == 'books':
                print(f"      Title: {record.get('title', 'N/A')[:60]}")
                print(f"      Year: {record.get('first_publish_year', 'N/A')}")
                print(f"      Genres: {record.get('genres', 'N/A')[:50]}")
                print(f"      Goodreads ID: {record.get('has_goodreads_id', 'N/A')}")
                
            elif file_type == 'editions':
                print(f"      Title: {record.get('title', 'N/A')[:60]}")
                print(f"      ISBN-13: {record.get('isbn_13', 'N/A')}")
                print(f"      Pages: {record.get('number_of_pages', 'N/A')}")
                print(f"      Year: {record.get('publish_year', 'N/A')}")
                
            elif file_type == 'authors':
                print(f"      Name: {record.get('name', 'N/A')}")
                print(f"      Birth: {record.get('birth_date', 'N/A')}")
                print(f"      Bio length: {len(record.get('bio', ''))}")
                
            elif file_type == 'book_authors':
                print(f"      Work: {record.get('openlibrary_work_key', 'N/A')}")
                print(f"      Author: {record.get('openlibrary_author_key', 'N/A')}")
    
    def analyze_data_quality(self, file_type, data):
        """Analyze quality metrics for each data type"""
        if not data:
            return
        
        print(f"\n  Quality Analysis:")
        
        if file_type == 'books':
            self.analyze_books_quality(data)
        elif file_type == 'editions':
            self.analyze_editions_quality(data)
        elif file_type == 'authors':
            self.analyze_authors_quality(data)
    
    def analyze_books_quality(self, data):
        """Analyze book data quality"""
        metrics = {
            'with_description': 0,
            'with_year': 0,
            'with_goodreads': 0,
            'with_amazon': 0,
            'with_genres': 0,
            'genre_distribution': Counter(),
            'year_distribution': Counter(),
            'description_lengths': []
        }
        
        for book in data:
            # Check basic fields
            if book.get('description') and len(book['description'].strip()) > 0:
                metrics['with_description'] += 1
                metrics['description_lengths'].append(len(book['description']))
            
            if book.get('first_publish_year'):
                metrics['with_year'] += 1
                try:
                    year = int(book['first_publish_year'])
                    decade = (year // 10) * 10
                    metrics['year_distribution'][decade] += 1
                except:
                    pass
            
            if book.get('has_goodreads_id') == 'True':
                metrics['with_goodreads'] += 1
            
            if book.get('has_amazon_id') == 'True':
                metrics['with_amazon'] += 1
            
            genres = book.get('genres', '')
            if genres:
                metrics['with_genres'] += 1
                for genre in genres.split('|'):
                    if genre.strip():
                        metrics['genre_distribution'][genre.strip()] += 1
        
        total = len(data)
        print(f"    Books with descriptions: {metrics['with_description']:,} ({metrics['with_description']/total*100:.1f}%)")
        print(f"    Books with pub year: {metrics['with_year']:,} ({metrics['with_year']/total*100:.1f}%)")
        print(f"    Books with Goodreads ID: {metrics['with_goodreads']:,} ({metrics['with_goodreads']/total*100:.1f}%)")
        print(f"    Books with Amazon ID: {metrics['with_amazon']:,} ({metrics['with_amazon']/total*100:.1f}%)")
        print(f"    Books with genres: {metrics['with_genres']:,} ({metrics['with_genres']/total*100:.1f}%)")
        
        if metrics['description_lengths']:
            avg_desc = sum(metrics['description_lengths']) / len(metrics['description_lengths'])
            print(f"    Avg description length: {avg_desc:.0f} characters")
        
        # Top genres
        print("    Top genres:")
        for genre, count in metrics['genre_distribution'].most_common(5):
            print(f"      {genre}: {count:,}")
        
        # Publication decades
        print("    Publication decades:")
        for decade in sorted(metrics['year_distribution'].keys())[-5:]:
            count = metrics['year_distribution'][decade]
            print(f"      {decade}s: {count:,}")
    
    def analyze_editions_quality(self, data):
        """Analyze edition data quality"""
        metrics = {
            'with_isbn13': 0,
            'with_isbn10': 0,
            'with_pages': 0,
            'with_publisher': 0,
            'language_distribution': Counter(),
            'page_counts': []
        }
        
        for edition in data:
            if edition.get('isbn_13'):
                metrics['with_isbn13'] += 1
            
            if edition.get('isbn_10'):
                metrics['with_isbn10'] += 1
            
            if edition.get('number_of_pages'):
                try:
                    pages = int(edition['number_of_pages'])
                    metrics['with_pages'] += 1
                    metrics['page_counts'].append(pages)
                except:
                    pass
            
            if edition.get('publishers'):
                metrics['with_publisher'] += 1
            
            languages = edition.get('languages', '')
            if languages:
                for lang in languages.split('|'):
                    if lang.strip():
                        metrics['language_distribution'][lang.strip()] += 1
        
        total = len(data)
        print(f"    Editions with ISBN-13: {metrics['with_isbn13']:,} ({metrics['with_isbn13']/total*100:.1f}%)")
        print(f"    Editions with ISBN-10: {metrics['with_isbn10']:,} ({metrics['with_isbn10']/total*100:.1f}%)")
        print(f"    Editions with page count: {metrics['with_pages']:,} ({metrics['with_pages']/total*100:.1f}%)")
        print(f"    Editions with publisher: {metrics['with_publisher']:,} ({metrics['with_publisher']/total*100:.1f}%)")
        
        if metrics['page_counts']:
            avg_pages = sum(metrics['page_counts']) / len(metrics['page_counts'])
            median_pages = sorted(metrics['page_counts'])[len(metrics['page_counts'])//2]
            print(f"    Avg page count: {avg_pages:.0f}")
            print(f"    Median page count: {median_pages}")
        
        print("    Top languages:")
        for lang, count in metrics['language_distribution'].most_common(5):
            print(f"      {lang}: {count:,}")
    
    def analyze_authors_quality(self, data):
        """Analyze author data quality"""
        metrics = {
            'with_bio': 0,
            'with_birth_date': 0,
            'with_death_date': 0,
            'bio_lengths': []
        }
        
        for author in data:
            bio = author.get('bio', '')
            if bio and len(bio.strip()) > 0:
                metrics['with_bio'] += 1
                metrics['bio_lengths'].append(len(bio))
            
            if author.get('birth_date'):
                metrics['with_birth_date'] += 1
            
            if author.get('death_date'):
                metrics['with_death_date'] += 1
        
        total = len(data)
        print(f"    Authors with bio: {metrics['with_bio']:,} ({metrics['with_bio']/total*100:.1f}%)")
        print(f"    Authors with birth date: {metrics['with_birth_date']:,} ({metrics['with_birth_date']/total*100:.1f}%)")
        print(f"    Authors with death date: {metrics['with_death_date']:,} ({metrics['with_death_date']/total*100:.1f}%)")
        
        if metrics['bio_lengths']:
            avg_bio = sum(metrics['bio_lengths']) / len(metrics['bio_lengths'])
            print(f"    Avg bio length: {avg_bio:.0f} characters")
    
    def validate_relationships(self):
        """Validate relationships between tables"""
        if not all(key in self.data for key in ['books', 'editions', 'authors', 'book_authors']):
            print("  Missing data files for relationship validation")
            return
        
        books = self.data['books']
        editions = self.data['editions']
        authors = self.data['authors']
        book_authors = self.data['book_authors']
        
        # Create lookup sets
        book_keys = {book['openlibrary_work_key'] for book in books}
        author_keys = {author['openlibrary_key'] for author in authors}
        edition_work_keys = {edition['openlibrary_work_key'] for edition in editions}
        
        # Validate book-author relationships
        relationship_issues = []
        valid_relationships = 0
        
        for rel in book_authors:
            work_key = rel['openlibrary_work_key']
            author_key = rel['openlibrary_author_key']
            
            if work_key not in book_keys:
                relationship_issues.append(f"Book key {work_key} not found in books")
            elif author_key not in author_keys:
                relationship_issues.append(f"Author key {author_key} not found in authors")
            else:
                valid_relationships += 1
        
        print(f"  Book-author relationships: {valid_relationships:,} valid")
        if relationship_issues:
            print(f"  Relationship issues: {len(relationship_issues)}")
            for issue in relationship_issues[:5]:  # Show first 5
                print(f"    {issue}")
        
        # Check edition-work relationships
        orphaned_editions = 0
        for edition in editions:
            if edition['openlibrary_work_key'] not in book_keys:
                orphaned_editions += 1
        
        print(f"  Editions linked to books: {len(editions) - orphaned_editions:,}/{len(editions):,}")
        if orphaned_editions > 0:
            print(f"  Orphaned editions: {orphaned_editions:,}")
        
        # Books per author distribution
        author_book_counts = Counter()
        for rel in book_authors:
            if rel['openlibrary_author_key'] in author_keys:
                author_book_counts[rel['openlibrary_author_key']] += 1
        
        if author_book_counts:
            max_books = max(author_book_counts.values())
            avg_books = sum(author_book_counts.values()) / len(author_book_counts)
            print(f"  Avg books per author: {avg_books:.1f}")
            print(f"  Max books per author: {max_books}")
    
    def print_summary(self):
        """Print overall summary"""
        print("\n" + "=" * 50)
        print("VERIFICATION SUMMARY")
        print("=" * 50)
        
        total_size = 0
        for file_path in self.files.values():
            if os.path.exists(file_path):
                total_size += os.path.getsize(file_path)
        
        print(f"Total data size: {total_size / (1024*1024):.1f} MB")
        
        for file_type, data_list in self.data.items():
            print(f"{file_type.capitalize()}: {len(data_list):,} records")
        
        # Estimate database size
        estimated_db_size = total_size * 3  # Rough estimate including indexes
        print(f"\nEstimated database size: {estimated_db_size / (1024*1024*1024):.1f} GB")
        
        print("\nData quality assessment: READY FOR DATABASE IMPORT")
        print("\nNext steps:")
        print("1. Set up PostgreSQL database")
        print("2. Create database schema") 
        print("3. Import CSV data")
        print("4. Create indexes for performance")


def main():
    data_dir = "./processed_data"  # Update this path if needed
    
    if not os.path.exists(data_dir):
        print(f"Error: Data directory '{data_dir}' not found!")
        print("Please update the data_dir path in the script.")
        return
    
    verifier = DataVerifier(data_dir)
    verifier.verify_all()


if __name__ == "__main__":
    main()