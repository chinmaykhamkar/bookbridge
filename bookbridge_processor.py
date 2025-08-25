#!/usr/bin/env python3
"""
BookBridge OpenLibrary Data Processor
Processes the full OpenLibrary dump and extracts high-quality books for BookBridge.
"""

import json
import csv
import re
import os
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Optional, Any
import argparse


class BookBridgeProcessor:
    def __init__(self, input_file: str, output_dir: str):
        self.input_file = input_file
        self.output_dir = output_dir
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        # Statistics tracking
        self.stats = {
            'total_lines_processed': 0,
            'works_processed': 0,
            'works_kept': 0,
            'editions_processed': 0,
            'editions_kept': 0,
            'authors_processed': 0,
            'authors_kept': 0,
            'errors': defaultdict(int)
        }
        
        # Data storage
        self.kept_works = {}  # work_key -> work_data
        self.kept_editions = {}  # edition_key -> edition_data
        self.kept_authors = {}  # author_key -> author_data
        
        # CSV writers
        self.csv_writers = {}
        self.csv_files = {}
        
        self.init_csv_writers()
    
    def init_csv_writers(self):
        """Initialize CSV files and writers for output"""
        
        # Books CSV
        books_file = open(os.path.join(self.output_dir, 'books.csv'), 'w', newline='', encoding='utf-8')
        books_writer = csv.writer(books_file)
        books_writer.writerow([
            'openlibrary_work_key', 'title', 'description', 'first_publish_year',
            'edition_count', 'cover_id', 'cover_url', 'subjects', 'genres',
            'has_goodreads_id', 'has_amazon_id', 'created_at'
        ])
        self.csv_files['books'] = books_file
        self.csv_writers['books'] = books_writer
        
        # Editions CSV
        editions_file = open(os.path.join(self.output_dir, 'editions.csv'), 'w', newline='', encoding='utf-8')
        editions_writer = csv.writer(editions_file)
        editions_writer.writerow([
            'openlibrary_edition_key', 'openlibrary_work_key', 'title',
            'isbn_10', 'isbn_13', 'publishers', 'publish_date', 'publish_year',
            'number_of_pages', 'languages', 'physical_format', 'cover_id', 'created_at'
        ])
        self.csv_files['editions'] = editions_file
        self.csv_writers['editions'] = editions_writer
        
        # Authors CSV
        authors_file = open(os.path.join(self.output_dir, 'authors.csv'), 'w', newline='', encoding='utf-8')
        authors_writer = csv.writer(authors_file)
        authors_writer.writerow([
            'openlibrary_key', 'name', 'personal_name', 'bio',
            'birth_date', 'death_date', 'photo_id', 'created_at'
        ])
        self.csv_files['authors'] = authors_file
        self.csv_writers['authors'] = authors_writer
        
        # Book-Author relationships CSV
        book_authors_file = open(os.path.join(self.output_dir, 'book_authors.csv'), 'w', newline='', encoding='utf-8')
        book_authors_writer = csv.writer(book_authors_file)
        book_authors_writer.writerow(['openlibrary_work_key', 'openlibrary_author_key', 'role'])
        self.csv_files['book_authors'] = book_authors_file
        self.csv_writers['book_authors'] = book_authors_writer
    
    def close_csv_writers(self):
        """Close all CSV files"""
        for file_obj in self.csv_files.values():
            file_obj.close()
    
    def process_file(self):
        """Main processing function - stream through the entire file"""
        print(f"Starting to process: {self.input_file}")
        print(f"Output directory: {self.output_dir}")
        print("-" * 60)
        
        start_time = datetime.now()
        
        try:
            with open(self.input_file, 'r', encoding='utf-8', errors='ignore') as f:
                for line_num, line in enumerate(f, 1):
                    self.stats['total_lines_processed'] = line_num
                    
                    # Progress indicator
                    if line_num % 100000 == 0:
                        self.print_progress(line_num, start_time)
                    
                    try:
                        record = self.parse_line(line.strip())
                        if record:
                            self.process_record(record)
                    except Exception as e:
                        self.stats['errors'][f'parse_error_{type(e).__name__}'] += 1
                        if self.stats['errors'][f'parse_error_{type(e).__name__}'] < 10:
                            print(f"Error on line {line_num}: {e}")
                        continue
                        
        except KeyboardInterrupt:
            print("\nProcessing interrupted by user.")
        except Exception as e:
            print(f"Fatal error: {e}")
        finally:
            self.close_csv_writers()
            self.print_final_stats(start_time)
    
    def parse_line(self, line: str) -> Optional[Dict]:
        """Parse a single line from the OpenLibrary dump"""
        parts = line.split('\t')
        if len(parts) < 5:
            return None
        
        record_type = parts[0]
        key = parts[1]
        revision = parts[2]
        timestamp = parts[3]
        json_data = '\t'.join(parts[4:])  # JSON might contain tabs
        
        # Only process relevant record types
        if record_type not in ['/type/work', '/type/edition', '/type/author']:
            return None
        
        try:
            data = json.loads(json_data)
            return {
                'type': record_type,
                'key': key,
                'revision': int(revision),
                'timestamp': timestamp,
                'data': data
            }
        except json.JSONDecodeError:
            return None
    
    def process_record(self, record: Dict):
        """Route records to appropriate processors"""
        if record['type'] == '/type/work':
            self.process_work(record)
        elif record['type'] == '/type/edition':
            self.process_edition(record)
        elif record['type'] == '/type/author':
            self.process_author(record)
    
    def process_work(self, record: Dict):
        """Process a work record"""
        self.stats['works_processed'] += 1
        data = record['data']
        
        if self.should_keep_work(data):
            work_data = self.extract_work_data(record)
            self.kept_works[record['key']] = work_data
            
            # Write to CSV immediately
            self.write_work_to_csv(work_data)
            
            # Write author relationships
            self.write_work_authors_to_csv(work_data)
            
            self.stats['works_kept'] += 1
    
    def process_edition(self, record: Dict):
        """Process an edition record"""
        self.stats['editions_processed'] += 1
        data = record['data']
        
        # Only keep editions if their work was kept
        work_keys = data.get('works', [])
        if not work_keys:
            return
        
        work_key = work_keys[0].get('key') if isinstance(work_keys[0], dict) else work_keys[0]
        
        if work_key in self.kept_works and self.should_keep_edition(data):
            edition_data = self.extract_edition_data(record, work_key)
            self.kept_editions[record['key']] = edition_data
            
            # Write to CSV immediately
            self.write_edition_to_csv(edition_data)
            
            self.stats['editions_kept'] += 1
    
    def process_author(self, record: Dict):
        """Process an author record"""
        self.stats['authors_processed'] += 1
        data = record['data']
        
        if self.should_keep_author(data):
            author_data = self.extract_author_data(record)
            self.kept_authors[record['key']] = author_data
            
            # Write to CSV immediately
            self.write_author_to_csv(author_data)
            
            self.stats['authors_kept'] += 1
    
    def should_keep_work(self, work_data: Dict) -> bool:
        """Apply quality filters to works"""
        try:
            # Must have meaningful title
            title = work_data.get('title', '').strip()
            if len(title) < 3:
                return False
            
            # Skip test/junk entries
            bad_indicators = ['TEST', 'DUPLICATE', 'DELETE', '[MERGED]', 'PLACEHOLDER', 'TEMP', '�']
            if any(indicator in title.upper() for indicator in bad_indicators):
                return False
            
            # Title must have reasonable ratio of letters
            letter_ratio = len(re.sub(r'[^a-zA-Z\s]', '', title)) / len(title)
            if letter_ratio < 0.4:
                return False
            
            # Must have some description
            description = work_data.get('description', {})
            if isinstance(description, dict):
                desc_text = description.get('value', '')
            else:
                desc_text = str(description) if description else ''
            
            if len(desc_text.strip()) < 50:
                return False
            
            # Must have authors
            authors = work_data.get('authors', [])
            if not authors:
                return False
            
            # Modified publication date logic
            first_publish_date = work_data.get('first_publish_date')
            if first_publish_date:
                # If date exists, apply 1940+ filter
                pub_year = self.extract_year(first_publish_date)
                if pub_year and pub_year < 1950:
                    return False
                # If we can't parse the year but field exists, keep it (might be malformed recent date)
            
            # If no publication date, keep the book (assume it might be recent)
            # Other quality filters will still apply
            return True
            
            # Popularity signals - must have at least one
            if not self.has_popularity_signals(work_data):
                return False
            
            return True
            
        except Exception as e:
            self.stats['errors']['work_filter_error'] += 1
            return False
    
    def has_popularity_signals(self, work_data: Dict) -> bool:
        """Check if work has signals indicating quality/popularity"""
        
        # Has external identifiers
        identifiers = work_data.get('identifiers', {})
        if identifiers.get('goodreads') or identifiers.get('amazon'):
            return True
        
        # Well-categorized (many subjects)
        subjects = work_data.get('subjects', [])
        if len(subjects) >= 3:
            return True
        
        # Multiple covers
        covers = work_data.get('covers', [])
        if len(covers) >= 2:
            return True
        
        # Check for English literature subjects (classic works)
        english_lit_indicators = ['fiction', 'literature', 'novel', 'classic', 'award']
        subject_text = ' '.join(subjects).lower()
        if any(indicator in subject_text for indicator in english_lit_indicators):
            if len(subjects) >= 2:
                return True
        
        return False
    
    def should_keep_edition(self, edition_data: Dict) -> bool:
        """Apply quality filters to editions"""
        try:
            # Must have ISBN
            isbn_13 = edition_data.get('isbn_13', [])
            isbn_10 = edition_data.get('isbn_10', [])
            if not isbn_13 and not isbn_10:
                return False
            
            # Must be English (or missing language info)
            languages = edition_data.get('languages', [])
            if languages:
                has_english = any(
                    lang.get('key') == '/languages/eng' if isinstance(lang, dict) else lang == 'eng'
                    for lang in languages
                )
                if not has_english:
                    return False
            
            # Must have publication date
            publish_date = edition_data.get('publish_date')
            if not publish_date:
                return False
            
            # Post-1940 filter (more lenient)
            pub_year = self.extract_year(publish_date)
            if not pub_year or pub_year < 1940:
                return False
            
            # Valid page count (lenient)
            pages = edition_data.get('number_of_pages')
            if pages and (pages < 30 or pages > 1500):
                return False
            
            # Must have publisher
            publishers = edition_data.get('publishers', [])
            if not publishers:
                return False
            
            return True
            
        except Exception as e:
            self.stats['errors']['edition_filter_error'] += 1
            return False
    
    def should_keep_author(self, author_data: Dict) -> bool:
        """Apply quality filters to authors"""
        try:
            # Must have name
            name = author_data.get('name', '').strip()
            if len(name) < 2:
                return False
            
            # Skip obvious test entries
            if any(bad in name.upper() for bad in ['TEST', 'DUPLICATE', 'DELETE', 'UNKNOWN']):
                return False
            
            return True
            
        except Exception as e:
            self.stats['errors']['author_filter_error'] += 1
            return False
    
    def extract_year(self, date_string: str) -> Optional[int]:
        """Extract year from various date formats"""
        if not date_string:
            return None
        
        # Try to find 4-digit year
        year_match = re.search(r'(\d{4})', str(date_string))
        if year_match:
            year = int(year_match.group(1))
            if 1800 <= year <= 2030:
                return year
        
        return None
    
    def extract_work_data(self, record: Dict) -> Dict:
        """Extract and clean work data"""
        data = record['data']
        
        # Get description
        description = data.get('description', {})
        if isinstance(description, dict):
            desc_text = description.get('value', '')
        else:
            desc_text = str(description) if description else ''
        
        # Get authors
        authors = data.get('authors', [])
        author_keys = []
        for author in authors:
            if isinstance(author, dict):
                if 'author' in author and isinstance(author['author'], dict):
                    author_keys.append(author['author'].get('key'))
                elif 'key' in author:
                    author_keys.append(author['key'])
            elif isinstance(author, str):
                author_keys.append(author)
        
        # Clean subjects and extract genres
        subjects = data.get('subjects', [])
        genres = self.extract_genres(subjects)
        
        # Get identifiers
        identifiers = data.get('identifiers', {})
        
        # Get covers
        covers = data.get('covers', [])
        cover_id = covers[0] if covers else None
        cover_url = f"https://covers.openlibrary.org/b/id/{cover_id}-L.jpg" if cover_id else None
        
        # Extract year from first_publish_date
        first_publish_date = data.get('first_publish_date')
        first_publish_year = self.extract_year(first_publish_date) if first_publish_date else None
        
        return {
            'openlibrary_work_key': record['key'],
            'title': data.get('title', '').strip(),
            'description': desc_text.strip(),
            'first_publish_year': first_publish_year,
            'edition_count': len(data.get('edition_key', [])),
            'cover_id': cover_id,
            'cover_url': cover_url,
            'subjects': '|'.join(subjects) if subjects else '',
            'genres': '|'.join(genres) if genres else '',
            'has_goodreads_id': bool(identifiers.get('goodreads')),
            'has_amazon_id': bool(identifiers.get('amazon')),
            'author_keys': [key for key in author_keys if key],
            'created_at': datetime.now().isoformat()
        }
    
    def extract_edition_data(self, record: Dict, work_key: str) -> Dict:
        """Extract and clean edition data"""
        data = record['data']
        
        # Get ISBNs
        isbn_10_list = data.get('isbn_10', [])
        isbn_13_list = data.get('isbn_13', [])
        
        # Get languages
        languages = data.get('languages', [])
        lang_codes = []
        for lang in languages:
            if isinstance(lang, dict):
                key = lang.get('key', '')
                lang_codes.append(key.replace('/languages/', ''))
            else:
                lang_codes.append(str(lang))
        
        # Get covers
        covers = data.get('covers', [])
        cover_id = covers[0] if covers else None
        
        # Get publication year
        publish_date = data.get('publish_date', '')
        publish_year = self.extract_year(publish_date)
        
        return {
            'openlibrary_edition_key': record['key'],
            'openlibrary_work_key': work_key,
            'title': data.get('title', '').strip(),
            'isbn_10': '|'.join(isbn_10_list) if isbn_10_list else '',
            'isbn_13': '|'.join(isbn_13_list) if isbn_13_list else '',
            'publishers': '|'.join(data.get('publishers', [])),
            'publish_date': publish_date,
            'publish_year': publish_year,
            'number_of_pages': data.get('number_of_pages'),
            'languages': '|'.join(lang_codes),
            'physical_format': data.get('physical_format', ''),
            'cover_id': cover_id,
            'created_at': datetime.now().isoformat()
        }
    
    def extract_author_data(self, record: Dict) -> Dict:
        """Extract and clean author data"""
        data = record['data']
        
        # Get bio
        bio = data.get('bio', {})
        if isinstance(bio, dict):
            bio_text = bio.get('value', '')
        else:
            bio_text = str(bio) if bio else ''
        
        # Get photos
        photos = data.get('photos', [])
        photo_id = photos[0] if photos else None
        
        return {
            'openlibrary_key': record['key'],
            'name': data.get('name', '').strip(),
            'personal_name': data.get('personal_name', '').strip(),
            'bio': bio_text.strip(),
            'birth_date': data.get('birth_date', ''),
            'death_date': data.get('death_date', ''),
            'photo_id': photo_id,
            'created_at': datetime.now().isoformat()
        }
    
    def extract_genres(self, subjects: List[str]) -> List[str]:
        """Extract clean genres from subjects list"""
        if not subjects:
            return []
        
        # Common genre mappings
        genre_patterns = {
            'fiction': ['fiction', 'novel', 'story', 'stories'],
            'fantasy': ['fantasy', 'magic', 'wizards', 'dragons'],
            'science_fiction': ['science fiction', 'sci-fi', 'space', 'alien'],
            'mystery': ['mystery', 'detective', 'crime', 'thriller'],
            'romance': ['romance', 'love story', 'romantic'],
            'horror': ['horror', 'supernatural', 'ghost', 'vampire'],
            'biography': ['biography', 'memoir', 'autobiography'],
            'history': ['history', 'historical'],
            'philosophy': ['philosophy', 'ethics'],
            'religion': ['religion', 'spiritual', 'faith'],
            'science': ['science', 'physics', 'chemistry', 'biology'],
            'business': ['business', 'economics', 'finance'],
            'self_help': ['self-help', 'personal development', 'motivation'],
            'travel': ['travel', 'guide', 'tourism'],
            'cooking': ['cooking', 'recipes', 'food'],
            'art': ['art', 'painting', 'sculpture', 'design'],
            'poetry': ['poetry', 'poems', 'verse']
        }
        
        genres = set()
        subject_text = ' '.join(subjects).lower()
        
        for genre, patterns in genre_patterns.items():
            if any(pattern in subject_text for pattern in patterns):
                genres.add(genre)
        
        # Always include 'fiction' or 'non-fiction' if we can determine it
        if any(pattern in subject_text for pattern in ['fiction', 'novel', 'story']):
            genres.add('fiction')
        elif genres and 'fiction' not in genres:
            genres.add('non_fiction')
        
        return list(genres)
    
    def write_work_to_csv(self, work_data: Dict):
        """Write work data to CSV"""
        self.csv_writers['books'].writerow([
            work_data['openlibrary_work_key'],
            work_data['title'],
            work_data['description'],
            work_data['first_publish_year'],
            work_data['edition_count'],
            work_data['cover_id'],
            work_data['cover_url'],
            work_data['subjects'],
            work_data['genres'],
            work_data['has_goodreads_id'],
            work_data['has_amazon_id'],
            work_data['created_at']
        ])
    
    def write_work_authors_to_csv(self, work_data: Dict):
        """Write work-author relationships to CSV"""
        for author_key in work_data['author_keys']:
            self.csv_writers['book_authors'].writerow([
                work_data['openlibrary_work_key'],
                author_key,
                'author'
            ])
    
    def write_edition_to_csv(self, edition_data: Dict):
        """Write edition data to CSV"""
        self.csv_writers['editions'].writerow([
            edition_data['openlibrary_edition_key'],
            edition_data['openlibrary_work_key'],
            edition_data['title'],
            edition_data['isbn_10'],
            edition_data['isbn_13'],
            edition_data['publishers'],
            edition_data['publish_date'],
            edition_data['publish_year'],
            edition_data['number_of_pages'],
            edition_data['languages'],
            edition_data['physical_format'],
            edition_data['cover_id'],
            edition_data['created_at']
        ])
    
    def write_author_to_csv(self, author_data: Dict):
        """Write author data to CSV"""
        self.csv_writers['authors'].writerow([
            author_data['openlibrary_key'],
            author_data['name'],
            author_data['personal_name'],
            author_data['bio'],
            author_data['birth_date'],
            author_data['death_date'],
            author_data['photo_id'],
            author_data['created_at']
        ])
    
    def print_progress(self, line_num: int, start_time: datetime):
        """Print progress update"""
        elapsed = datetime.now() - start_time
        rate = line_num / elapsed.total_seconds() if elapsed.total_seconds() > 0 else 0
        
        print(f"Processed {line_num:,} lines | "
              f"Works: {self.stats['works_kept']:,}/{self.stats['works_processed']:,} | "
              f"Rate: {rate:,.0f} lines/sec | "
              f"Elapsed: {elapsed}")
    
    def print_final_stats(self, start_time: datetime):
        """Print final processing statistics"""
        elapsed = datetime.now() - start_time
        
        print("\n" + "="*60)
        print("PROCESSING COMPLETE")
        print("="*60)
        
        print(f"Total processing time: {elapsed}")
        print(f"Total lines processed: {self.stats['total_lines_processed']:,}")
        print()
        
        print("WORKS:")
        print(f"  Processed: {self.stats['works_processed']:,}")
        print(f"  Kept: {self.stats['works_kept']:,}")
        if self.stats['works_processed'] > 0:
            print(f"  Retention rate: {self.stats['works_kept']/self.stats['works_processed']*100:.1f}%")
        
        print("\nEDITIONS:")
        print(f"  Processed: {self.stats['editions_processed']:,}")
        print(f"  Kept: {self.stats['editions_kept']:,}")
        if self.stats['editions_processed'] > 0:
            print(f"  Retention rate: {self.stats['editions_kept']/self.stats['editions_processed']*100:.1f}%")
        
        print("\nAUTHORS:")
        print(f"  Processed: {self.stats['authors_processed']:,}")
        print(f"  Kept: {self.stats['authors_kept']:,}")
        if self.stats['authors_processed'] > 0:
            print(f"  Retention rate: {self.stats['authors_kept']/self.stats['authors_processed']*100:.1f}%")
        
        if self.stats['errors']:
            print("\nERRORS:")
            for error_type, count in sorted(self.stats['errors'].items()):
                print(f"  {error_type}: {count:,}")
        
        print(f"\nOutput files created in: {self.output_dir}")
        print("- books.csv")
        print("- editions.csv") 
        print("- authors.csv")
        print("- book_authors.csv")


def main():
    parser = argparse.ArgumentParser(description='Process OpenLibrary dump for BookBridge')
    parser.add_argument('input_file', help='Path to OpenLibrary dump file')
    parser.add_argument('--output-dir', default='./bookbridge_data', 
                       help='Output directory for processed data')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.input_file):
        print(f"Error: Input file '{args.input_file}' not found!")
        return
    
    processor = BookBridgeProcessor(args.input_file, args.output_dir)
    processor.process_file()


if __name__ == "__main__":
    main()