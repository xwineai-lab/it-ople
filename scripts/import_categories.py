#!/usr/bin/env python3
"""
Import OPLE category CSV into the database.
CSV columns: it_id, it_name, category_id, category_depth

Usage:
  python scripts/import_categories.py /path/to/csv
  # or via API: POST /api/categories/import  (multipart file upload)
"""
import csv
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'api'))
from database import init_db, SessionLocal, Category, ProductCategory


def parse_depth(depth_str: str):
    """Parse 'category_depth' like '대상별 > 부모님 > 혈행/혈압/당뇨' into levels."""
    parts = [p.strip() for p in depth_str.split('>')]
    level1 = parts[0] if len(parts) >= 1 else None
    level2 = parts[1] if len(parts) >= 2 else None
    level3 = parts[2] if len(parts) >= 3 else None
    depth = len(parts)
    return level1, level2, level3, depth


def make_shopify_tags(level1, level2, level3):
    """Generate Shopify tag values from category levels."""
    tag_cat = f"cat:{level1}" if level1 else None
    tag_sub = f"sub:{level2}" if level2 else None
    tag_sub2 = f"sub2:{level3}" if level3 else None
    return tag_cat, tag_sub, tag_sub2


def import_csv(csv_path: str, batch_size: int = 2000):
    """Import category CSV into DB. Returns stats dict."""
    init_db()
    db = SessionLocal()

    stats = {'categories_created': 0, 'mappings_created': 0, 'rows_processed': 0, 'skipped': 0}

    # Phase 1: Collect unique categories and product-category pairs
    cat_map = {}       # category_id → {depth_path, level1, level2, level3, depth, count}
    pc_pairs = set()   # (it_id, category_id) unique tuples

    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        header = next(reader)  # skip header
        for row in reader:
            if len(row) < 4:
                stats['skipped'] += 1
                continue
            it_id = row[0].strip()
            # it_name = row[1]  # not stored in category tables
            cat_id = row[2].strip()
            depth_path = row[3].strip()

            if not it_id or not cat_id or not depth_path:
                stats['skipped'] += 1
                continue

            stats['rows_processed'] += 1

            # Build category info
            if cat_id not in cat_map:
                l1, l2, l3, d = parse_depth(depth_path)
                t_cat, t_sub, t_sub2 = make_shopify_tags(l1, l2, l3)
                cat_map[cat_id] = {
                    'depth_path': depth_path, 'level1': l1, 'level2': l2, 'level3': l3,
                    'depth': d, 'count': 0,
                    'shopify_tag_cat': t_cat, 'shopify_tag_sub': t_sub, 'shopify_tag_sub2': t_sub2
                }
            cat_map[cat_id]['count'] += 1
            pc_pairs.add((it_id, cat_id))

    print(f"Parsed {stats['rows_processed']} rows → {len(cat_map)} categories, {len(pc_pairs)} product-category links")

    # Phase 2: Upsert categories
    existing_cats = {c.category_id for c in db.query(Category.category_id).all()}
    new_cats = []
    for cat_id, info in cat_map.items():
        if cat_id not in existing_cats:
            new_cats.append(Category(
                category_id=cat_id,
                depth_path=info['depth_path'],
                level1=info['level1'],
                level2=info['level2'],
                level3=info['level3'],
                depth=info['depth'],
                product_count=info['count'],
                shopify_tag_cat=info['shopify_tag_cat'],
                shopify_tag_sub=info['shopify_tag_sub'],
                shopify_tag_sub2=info['shopify_tag_sub2'],
            ))
        else:
            # Update product_count for existing
            db.query(Category).filter(Category.category_id == cat_id).update(
                {'product_count': info['count']}
            )

    if new_cats:
        db.bulk_save_objects(new_cats)
        db.commit()
        stats['categories_created'] = len(new_cats)
        print(f"Created {len(new_cats)} new categories")

    # Phase 3: Insert product-category mappings in batches
    existing_pcs = set()
    for row in db.query(ProductCategory.it_id, ProductCategory.category_id).all():
        existing_pcs.add((row[0], row[1]))

    new_pcs = []
    for it_id, cat_id in pc_pairs:
        if (it_id, cat_id) not in existing_pcs:
            new_pcs.append(ProductCategory(it_id=it_id, category_id=cat_id))

    if new_pcs:
        for i in range(0, len(new_pcs), batch_size):
            batch = new_pcs[i:i + batch_size]
            db.bulk_save_objects(batch)
            db.commit()
            print(f"  Inserted batch {i // batch_size + 1}: {len(batch)} mappings")
        stats['mappings_created'] = len(new_pcs)
        print(f"Created {len(new_pcs)} product-category mappings")

    db.close()
    return stats


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python import_categories.py <csv_path>")
        sys.exit(1)
    result = import_csv(sys.argv[1])
    print(f"\nDone! {result}")
