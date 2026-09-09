-- Migration: hold_sweeper
-- Created at: Wed, 09 Sep 2026 21:07:31 GMT

-- Write your forward-only DDL here

-- The sweeper asks "does a live payment still own this hold?" once per candidate
-- hold. Without this that is a seq scan of payments per row.
CREATE INDEX payments_by_hold ON payments (hold_id) WHERE hold_id IS NOT NULL;