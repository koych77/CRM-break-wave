-- The subscription is chosen explicitly: by the parent in the form, then by staff.
ALTER TABLE students ADD COLUMN tariff_code TEXT;
ALTER TABLE registration_requests ADD COLUMN preferred_tariff TEXT;

-- A first month that costs as much as a subscription is that whole subscription, counted from the 1st.
-- Only lesson counts change: amounts, statuses and every other row stay as they are.
UPDATE payments
SET is_partial = 0,
    lessons_count = CASE tariff_code WHEN '8' THEN 8 WHEN '12' THEN 12 WHEN '16' THEN 16 END
WHERE kind = 'subscription'
  AND is_partial = 1
  AND tariff_code IN ('8', '12', '16')
  AND amount >= CASE tariff_code WHEN '8' THEN 150 WHEN '12' THEN 180 WHEN '16' THEN 200 END;

-- Each child keeps the subscription of their latest full month.
UPDATE students
SET tariff_code = (
  SELECT p.tariff_code FROM payments p
  WHERE p.student_id = students.id AND p.kind = 'subscription' AND p.is_partial = 0 AND p.tariff_code IN ('8', '12', '16')
  ORDER BY p.period_start DESC LIMIT 1
);
