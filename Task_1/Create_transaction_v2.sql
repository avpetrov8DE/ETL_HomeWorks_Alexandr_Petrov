CREATE TABLE transactions_v2 (
    call_id UTF8 NOT NULL,
    call_time Timestamp NOT NULL,
    client_id UTF8 NOT NULL,
    region_code UTF8 NOT NULL,
    campaign_type UTF8 NOT NULL,
    call_status UTF8 NOT NULL,
    client_response UTF8,
    duration_sec Uint32 NOT NULL,
    follow_up_required Bool NOT NULL,
    PRIMARY KEY (call_id)
);