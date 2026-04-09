# Vendor Knowledge: Amazon Connect

## API Details
- **Base URL:** Regional — `connect.{region}.amazonaws.com`
- **Auth:** IAM AssumeRole with ExternalId (confused deputy prevention)
- **Rate limits:** 2 req/sec per account per region (documented). We use 70% = 1.4 req/sec.
- **Pagination:** NextToken-based, 100 results per page
- **Changelog URL:** https://aws.amazon.com/about-aws/whats-new/?whats-new-content-all.sort-by=item.additionalFields.postDateTime&whats-new-content-all.sort-order=desc&awsf.whats-new-categories=general-products%23amazon-connect

## Known Quirks
- SearchContacts only returns contacts AFTER they disconnect — active contacts are invisible
- CTR fields vary by channel (VOICE has Recording, CHAT has ChatMetrics, TASK has Name/Description)
- LexBotInteraction data only appears in Kinesis stream CTRs, not in DescribeContact API response
- Contact Lens requires explicit enablement on the instance — not on by default
- Contact Lens transcript data has a ~30 second delay after contact disconnects
- Contact flow Lex blocks cannot be added via API — must use Connect visual editor
- STS credentials are per-region — multi-region instances need separate connector accounts

## Sandbox Setup
- **Instance:** arn:aws:connect:us-west-2:291925528464:instance/47f7baa6-ffb0-4afd-a025-9d277271e699
- **CF templates:** s3://veratrace-cloudformation/amazon-connect-sandbox.yaml (write perms), amazon-connect.yaml (read-only)
- **Lex bot:** VeratraceSandboxBot (4 intents: ResetPassword, CheckBalance, BillingDispute, FallbackIntent)
- **Contact flow:** Veratrace-Lex-Demo (Lex routing with bot-resolved/transfer branches)
- **Cost:** ~$16/month for warming (5 contacts/hour × 10 hours × 22 days)

## PII Fields
- CustomerEndpoint (phone number, email)
- Attributes (may contain customer data set by contact flows)
- Contact Lens Transcript (full conversation text)

## AI/Bot Features
- **Lex V2 bots:** Integrated via contact flow "Get customer input" block. CTR includes LexBotInteraction with intent, confidence, slots.
- **Contact Lens:** Real-time and post-call analytics — sentiment, categories, issues, transcript. Requires instance-level enablement.
- **AI attribution in CTR:** No agent ARN = SYSTEM (could be bot, IVR, or dropped call). Structured attributes (aiHandled, aiConfidence) set by flow carry the real attribution.

## Schema Drift History
- 2026-04-09: Initial schema captured (EXPECTED_CTR_FIELDS in schema.py)
