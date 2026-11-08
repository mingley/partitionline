    // Qualification-only wrapper. Both benchmark arms receive the same code.
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

