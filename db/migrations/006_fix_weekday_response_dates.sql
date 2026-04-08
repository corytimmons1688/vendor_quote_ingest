-- Migration 006: Fix email_date and response_date for alternate filename format
--
-- Files named DayOfWeek_DD_Mon_YYYY_HH_Vendor_... (e.g. Mon_15_Dec_2025_23_Ross_...)
-- had their email_date parsed as just the weekday string ("Mon", "Tue", etc.)
-- because parse_filename_metadata() only handled the standard YYYY-MM-DD_HHMMSS format.
--
-- This migration extracts the correct date from source_file for all affected rows.

-- Ross
UPDATE est_ex_br_ross
SET email_date = to_date(
        split_part(source_file, '_', 2) || ' ' ||
        split_part(source_file, '_', 3) || ' ' ||
        split_part(source_file, '_', 4),
        'DD Mon YYYY'
    )::text,
    response_date = to_date(
        split_part(source_file, '_', 2) || ' ' ||
        split_part(source_file, '_', 3) || ' ' ||
        split_part(source_file, '_', 4),
        'DD Mon YYYY'
    )::text
WHERE email_date IN ('Mon','Tue','Wed','Thu','Fri','Sat','Sun');

-- Tedpack
UPDATE est_ex_br_tedpack
SET email_date = to_date(
        split_part(source_file, '_', 2) || ' ' ||
        split_part(source_file, '_', 3) || ' ' ||
        split_part(source_file, '_', 4),
        'DD Mon YYYY'
    )::text,
    response_date = to_date(
        split_part(source_file, '_', 2) || ' ' ||
        split_part(source_file, '_', 3) || ' ' ||
        split_part(source_file, '_', 4),
        'DD Mon YYYY'
    )::text
WHERE email_date IN ('Mon','Tue','Wed','Thu','Fri','Sat','Sun');

-- Dazpak
UPDATE est_ex_br_dazpak
SET email_date = to_date(
        split_part(source_file, '_', 2) || ' ' ||
        split_part(source_file, '_', 3) || ' ' ||
        split_part(source_file, '_', 4),
        'DD Mon YYYY'
    )::text,
    response_date = to_date(
        split_part(source_file, '_', 2) || ' ' ||
        split_part(source_file, '_', 3) || ' ' ||
        split_part(source_file, '_', 4),
        'DD Mon YYYY'
    )::text
WHERE email_date IN ('Mon','Tue','Wed','Thu','Fri','Sat','Sun');
