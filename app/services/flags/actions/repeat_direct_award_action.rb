# frozen_string_literal: true

module Flags
  module Actions
    class RepeatDirectAwardAction
      FLAG_TYPE    = "A1_REPEAT_DIRECT_AWARD"
      SCORE        = 50
      SEVERITY     = "high"
      MIN_AWARDS   = 3
      WINDOW_DAYS  = 1096 # ~36 months

      DIRECT_AWARD_PATTERN = "%ajuste%direto%"

      def call
        flagged_contract_ids = qualifying_contract_ids
        upsert_flags(flagged_contract_ids)
        cleanup_stale_flags(flagged_contract_ids)
        flagged_contract_ids.size
      end

      private

      def qualifying_contract_ids
        # Group direct awards by (authority, supplier), find groups with 3+
        # awards whose publication dates all fall within a 36-month window.
        groups = direct_awards
          .joins(:contract_winners)
          .where.not(publication_date: nil)
          .group("contracts.contracting_entity_id, contract_winners.entity_id")
          .having(Arel.sql("COUNT(*) >= #{MIN_AWARDS}"))
          .pluck(
            Arel.sql("contracts.contracting_entity_id"),
            Arel.sql("contract_winners.entity_id"),
            Arel.sql("MIN(contracts.publication_date)"),
            Arel.sql("MAX(contracts.publication_date)"),
            Arel.sql("COUNT(*)")
          )

        qualifying = groups.select do |_auth, _sup, min_date, max_date, _count|
          (Date.parse(max_date.to_s) - Date.parse(min_date.to_s)).to_i <= WINDOW_DAYS
        end

        return [] if qualifying.empty?

        qualifying.flat_map do |auth_id, sup_id, _min, _max, count|
          ids = direct_awards
            .joins(:contract_winners)
            .where.not(publication_date: nil)
            .where(contracting_entity_id: auth_id, contract_winners: { entity_id: sup_id })
            .pluck(:id)
          # Store group metadata for use when building flag rows
          ids.map { |id| [ id, auth_id, sup_id, count ] }
        end
      end

      def upsert_flags(flagged_rows)
        return if flagged_rows.empty?

        now = Time.current
        rows = flagged_rows.map do |contract_id, auth_id, sup_id, award_count|
          {
            contract_id: contract_id,
            flag_type: FLAG_TYPE,
            severity: SEVERITY,
            score: SCORE,
            details: {
              "authority_id"  => auth_id,
              "supplier_id"   => sup_id,
              "award_count"   => award_count,
              "rule"          => "A1 repeat direct award: #{award_count} awards within 36 months"
            },
            fired_at: now,
            created_at: now,
            updated_at: now
          }
        end

        Flag.upsert_all(rows, unique_by: :index_flags_on_contract_id_and_flag_type)
      end

      def cleanup_stale_flags(flagged_rows)
        contract_ids = flagged_rows.map(&:first)
        stale_scope = Flag.where(flag_type: FLAG_TYPE)
        if contract_ids.empty?
          stale_scope.delete_all
        else
          stale_scope.where.not(contract_id: contract_ids).delete_all
        end
      end

      def direct_awards
        Contract.where(Contract.arel_table[:procedure_type].matches(DIRECT_AWARD_PATTERN))
      end
    end
  end
end
