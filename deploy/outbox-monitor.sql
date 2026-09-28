-- Run as the schema owner after provider installation; create the login separately.
-- Replace public and knd_monitor for your deployment. No payload access or write privileges.
GRANT USAGE ON SCHEMA public TO knd_monitor;
GRANT SELECT (subject, created_at, next_attempt_at, attempts, ordering_key, user_sequence,
              publication_may_have_occurred)
  ON public.kc_nats_outbox TO knd_monitor;
GRANT SELECT ON public.kc_nats_discard_audit TO knd_monitor;
