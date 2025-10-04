# SUPPLY-SIGNALS State Summary

**Current Version:** v0.1.0-phase0  
**Phase Complete:** Baseline Docker pipeline verified end-to-end.  
**Next Phase:** SEC + Press-Release MVP (Phase 1)  
**Services:** data_ingest, normalize_enrich, signal_detect, alert_engine  
**Core Schema:** RawEvent → Fact → Signal  

Notes:
- Redis queues verified working.
- Alert engine outputs heartbeat messages.
- Ready for ingestion of real data sources (SEC filings, PR wires).
