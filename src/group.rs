use crate::consumer::{Consumer, ConsumerConfig, FetchedRecord};
use crate::error::{self, Error, Result};
use crate::protocol::api_keys::{
    FIND_COORDINATOR, HEARTBEAT, JOIN_GROUP, OFFSET_COMMIT, OFFSET_FETCH, SYNC_GROUP,
};
use crate::protocol::group::{
    decode_assignment, decode_find_coordinator_response, decode_heartbeat_response,
    decode_join_group_response, decode_offset_commit_response, decode_offset_fetch_response,
    decode_sync_group_response, encode_assignment, encode_find_coordinator_request,
    encode_heartbeat_request, encode_join_group_request, encode_offset_commit_request,
    encode_offset_fetch_request, encode_subscription, encode_sync_group_request,
};

pub struct ConsumerGroup {
    consumer: Consumer,
    group_id: String,
    member_id: String,
    generation_id: i32,
    topic: String,
}

impl ConsumerGroup {
    pub async fn join(
        cfg: ConsumerConfig,
        group_id: impl Into<String>,
        topic: impl Into<String>,
    ) -> Result<Self> {
        let group_id = group_id.into();
        let topic = topic.into();
        let mut consumer = Consumer::new(cfg).await?;
        let timeout = std::time::Duration::from_secs(30);

        let body = consumer
            .conn_mut()
            .roundtrip(
                FIND_COORDINATOR,
                2,
                |buf| encode_find_coordinator_request(buf, &group_id),
                timeout,
            )
            .await?;
        let (err, _node, _host, _port) = decode_find_coordinator_response(&mut body.clone())?;
        if err != 0 {
            return Err(Error::broker(err, "FindCoordinator"));
        }

        let metadata = encode_subscription(std::slice::from_ref(&topic));
        let mut member_id = String::new();
        let (error, generation, _proto, leader, assigned_id, members) = {
            let body = consumer
                .conn_mut()
                .roundtrip(
                    JOIN_GROUP,
                    5,
                    |buf| {
                        encode_join_group_request(
                            buf, &group_id, 10_000, &member_id, "consumer", "range", &metadata,
                        )
                    },
                    timeout,
                )
                .await?;
            decode_join_group_response(&mut body.clone())?
        };
        member_id = assigned_id;
        let (error, generation, leader, members) = if error == error::MEMBER_ID_REQUIRED {
            let body = consumer
                .conn_mut()
                .roundtrip(
                    JOIN_GROUP,
                    5,
                    |buf| {
                        encode_join_group_request(
                            buf, &group_id, 10_000, &member_id, "consumer", "range", &metadata,
                        )
                    },
                    timeout,
                )
                .await?;
            let (e, g, _p, l, mid, m) = decode_join_group_response(&mut body.clone())?;
            member_id = mid;
            (e, g, l, m)
        } else {
            (error, generation, leader, members)
        };
        if error != 0 {
            return Err(Error::broker(error, "JoinGroup"));
        }

        let assignments = if leader == member_id {
            members
                .iter()
                .map(|m| (m.member_id.clone(), encode_assignment(&topic, &[0])))
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let body = consumer
            .conn_mut()
            .roundtrip(
                SYNC_GROUP,
                3,
                |buf| {
                    encode_sync_group_request(buf, &group_id, generation, &member_id, &assignments)
                },
                timeout,
            )
            .await?;
        let (err, assignment) = decode_sync_group_response(&mut body.clone())?;
        if err != 0 {
            return Err(Error::broker(err, "SyncGroup"));
        }
        let assigned = decode_assignment(&assignment)?;
        let hb = consumer
            .conn_mut()
            .roundtrip(
                HEARTBEAT,
                3,
                |buf| encode_heartbeat_request(buf, &group_id, generation, &member_id),
                timeout,
            )
            .await?;
        let hb_err = decode_heartbeat_response(&mut hb.clone())?;
        if hb_err != 0 {
            return Err(Error::broker(hb_err, "Heartbeat"));
        }

        for (t, parts) in assigned {
            for p in parts {
                let body = consumer
                    .conn_mut()
                    .roundtrip(
                        OFFSET_FETCH,
                        5,
                        |buf| encode_offset_fetch_request(buf, &group_id, &t, p),
                        timeout,
                    )
                    .await?;
                let committed = decode_offset_fetch_response(&mut body.clone()).unwrap_or(-1);
                let start = if committed < 0 { 0 } else { committed };
                consumer.assign(t.clone(), p, start).await?;
            }
        }

        Ok(Self {
            consumer,
            group_id,
            member_id,
            generation_id: generation,
            topic,
        })
    }

    pub async fn poll(&mut self) -> Result<Vec<FetchedRecord>> {
        self.consumer.fetch().await
    }

    pub async fn commit(&mut self) -> Result<()> {
        let timeout = std::time::Duration::from_secs(30);
        for (topic, part, next) in self.consumer.assignment().to_vec() {
            let offset = next.saturating_sub(0);
            let body = self
                .consumer
                .conn_mut()
                .roundtrip(
                    OFFSET_COMMIT,
                    7,
                    |buf| {
                        encode_offset_commit_request(
                            buf,
                            &self.group_id,
                            self.generation_id,
                            &self.member_id,
                            &topic,
                            part,
                            offset,
                        )
                    },
                    timeout,
                )
                .await?;
            let err = decode_offset_commit_response(&mut body.clone())?;
            if err != 0 {
                return Err(Error::broker(err, "OffsetCommit"));
            }
        }
        let _ = &self.topic;
        Ok(())
    }
}
