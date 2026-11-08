    // Qualification-only wrappers. Both benchmark arms receive the same code.
    #[doc(hidden)]
    pub fn __qualification_staged_sync(
        &mut self,
        record_count: u32,
        payload: &[u8],
        checksum: u32,
    ) -> Result<Append, Error> {
        let ticket = self.append_staged(record_count, payload, checksum)?;
        ticket.sync_data()?;
        self.commit_staged()
    }

    #[doc(hidden)]
    pub fn __qualification_group_stage(
        &mut self,
        groups: usize,
        payload: &[u8],
        checksum: u32,
    ) -> Result<u128, Error> {
        let mut elapsed = 0_u128;
        for _ in 0..groups {
            let mut tickets = Vec::with_capacity(8);
            let started = std::time::Instant::now();
            for _ in 0..8 {
                tickets.push(self.append_staged_grouped(1, payload, checksum)?);
            }
            elapsed = elapsed.saturating_add(started.elapsed().as_nanos());
            tickets
                .last()
                .ok_or(Error::InvalidEntry)?
                .sync_data()?;
            for _ in 0..8 {
                let _ = self.commit_staged()?;
            }
            let started = std::time::Instant::now();
            drop(tickets);
            elapsed = elapsed.saturating_add(started.elapsed().as_nanos());
        }
        Ok(elapsed)
    }

    #[doc(hidden)]
    pub fn __qualification_layout() -> (usize, usize) {
        (
            std::mem::size_of::<SyncFile>(),
            ownership::__qualification_lease_size(),
        )
    }

