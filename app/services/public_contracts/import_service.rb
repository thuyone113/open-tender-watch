# frozen_string_literal: true

module PublicContracts
  class ImportService
    def initialize(data_source_record)
      @ds = data_source_record
    end

    def call
      contracts = adapter.fetch_contracts
      contracts.each { |attrs| import_contract(attrs) }
      @ds.update!(status: :active, last_synced_at: Time.current, record_count: contracts.size)
    rescue => e
      @ds.update!(status: :error)
      raise
    end

    # Paginates through every page until the adapter returns no more records.
    # The adapter must support fetch_contracts(page:, limit:).
    # Optional: if the adapter responds to #total_count, it's used only for
    # progress reporting — not to control the loop.
    #
    # The adapter is memoized for the lifetime of this call so that stateful
    # adapters (e.g. TedClient scroll token, SnsClient year-window index) retain
    # their internal position across batches.
    #
    # When the adapter responds to #each_contract (e.g. PortalBaseClient), this
    # method delegates to call_streaming, which is dramatically faster:
    #   - Each source file downloaded exactly once (vs O(n²) re-downloads with paging)
    #   - Entities cached in memory (vs a DB round-trip per entity per row)
    #   - Contracts committed in transactional batches (vs one auto-commit per row)
    #   - Duplicate rows logged-and-skipped instead of crashing
    def call_all(limit: 100, progress: $stdout)
      return call_streaming(batch_size: limit, progress: progress) if adapter.respond_to?(:each_contract)

      total_known  = adapter.respond_to?(:total_count) ? adapter.total_count : nil
      imported     = 0
      page         = 1

      loop do
        batch = adapter.fetch_contracts(page: page, limit: limit)
        break if batch.empty?

        batch.each { |attrs| import_contract(attrs) }
        imported += batch.size
        page     += 1

        # Pace requests for rate-limited adapters (e.g. TED API)
        sleep adapter.inter_page_delay if adapter.respond_to?(:inter_page_delay)

        if progress
          if total_known
            progress.print "\r  #{imported}/#{total_known} imported (page #{page - 1})"
          else
            progress.print "\r  #{imported} imported (page #{page - 1})"
          end
          progress.flush
        end
      end

      progress&.puts "\n  Done — #{imported} records"
      @ds.update!(status: :active, last_synced_at: Time.current, record_count: imported)
    rescue => e
      @ds.update!(status: :error)
      raise
    end

    # Fast bulk import for adapters that support #each_contract.
    #
    # Three key optimisations over call_all's pagination loop:
    #   1. Single pass: each source file is downloaded exactly once. The
    #      paginated path re-downloads from the beginning on every page call,
    #      producing O(n²) network traffic for large datasets.
    #   2. Entity cache: buyer/winner entities are looked up once per unique NIF
    #      and stored in a Ruby hash. This turns O(contracts) DB round-trips into
    #      O(unique_entities) — typically a 100-200x reduction.
    #   3. Batch transactions: contracts are committed in groups of batch_size
    #      instead of one auto-commit per row. Each row within the batch is
    #      wrapped in a SAVEPOINT so a duplicate-key error on one row only rolls
    #      back that row, not the whole batch.
    def call_streaming(batch_size: 500, progress: $stdout)
      raise ArgumentError, "#{adapter.class} does not support #each_contract" unless adapter.respond_to?(:each_contract)

      total_known  = adapter.respond_to?(:total_count) ? adapter.total_count : nil
      imported     = 0
      skipped      = 0
      entity_cache = {}
      queue        = []

      flush = lambda do
        return if queue.empty?

        ApplicationRecord.transaction do
          queue.each do |raw|
            ApplicationRecord.transaction(requires_new: true) do
              import_contract_cached(raw, entity_cache)
              imported += 1
            end
          rescue ActiveRecord::RecordInvalid, ActiveRecord::RecordNotUnique => e
            skipped += 1
            Rails.logger.warn "[ImportService] duplicate skipped (#{adapter.class}): #{e.message.truncate(200)}"
          end
        end

        queue.clear

        if progress
          label = total_known ? "#{imported}/#{total_known}" : imported.to_s
          progress.print "\r  #{label} imported, #{skipped} skipped"
          progress.flush
        end
      end

      adapter.each_contract do |raw|
        queue << raw
        flush.call if queue.size >= batch_size
      end
      flush.call

      progress&.puts "\n  Done — #{imported} imported, #{skipped} skipped"
      @ds.update!(status: :active, last_synced_at: Time.current, record_count: imported)
    rescue => e
      @ds.update!(status: :error)
      raise
    end

    private

    CONTRACT_FIELDS = {
      object: "object",
      contract_type: "contract_type",
      procedure_type: "procedure_type",
      publication_date: "publication_date",
      celebration_date: "celebration_date",
      base_price: "base_price",
      total_effective_price: "total_effective_price",
      cpv_code: "cpv_code",
      location: "location"
    }.freeze

    # Memoized adapter — critical for stateful adapters (TedClient scroll token,
    # SnsClient year-window index) that need the same instance across all batches.
    def adapter
      @adapter ||= @ds.adapter
    end

    # Dedup strategy: the DB unique constraint on (external_id, country_code)
    # prevents duplicate contracts both within a single source (on re-import) and
    # across sources that share the same external_id namespace (e.g. Portal BASE and
    # QuemFatura.pt both use idcontrato). We still merge incoming data into existing
    # records to backfill missing fields without creating duplicates.
    def import_contract(attrs)
      attrs = normalize_contract_attrs(attrs)
      return if attrs["external_id"].blank? || attrs["object"].blank?

      contracting = find_or_create_entity(
        attrs.dig("contracting_entity", "tax_identifier"),
        attrs.dig("contracting_entity", "name"),
        is_public_body: attrs.dig("contracting_entity", "is_public_body") || false
      )
      return unless contracting

      winner_tax_ids = Array(attrs["winners"]).filter_map { |winner| winner["tax_identifier"].presence }
      contract = find_existing_contract(attrs, contracting, winner_tax_ids) || Contract.new
      merge_contract_attributes!(contract, attrs, contracting)
      contract.save! if contract.new_record? || contract.changed?

      Array(attrs["winners"]).each do |winner_attrs|
        winner = find_or_create_entity(
          winner_attrs["tax_identifier"],
          winner_attrs["name"],
          is_company: winner_attrs["is_company"] || false
        )
        next unless winner
        ContractWinner.find_or_create_by!(contract: contract, entity: winner)
      end
    end

    # Like import_contract but resolves entities through an in-memory cache to
    # avoid a DB round-trip for every entity reference on every row.
    def import_contract_cached(raw_attrs, entity_cache)
      attrs = normalize_contract_attrs(raw_attrs)
      return if attrs["external_id"].blank? || attrs["object"].blank?

      contracting = find_or_create_entity_cached(
        attrs.dig("contracting_entity", "tax_identifier"),
        attrs.dig("contracting_entity", "name"),
        is_public_body: attrs.dig("contracting_entity", "is_public_body") || false,
        cache: entity_cache
      )
      return unless contracting

      winner_tax_ids = Array(attrs["winners"]).filter_map { |w| w["tax_identifier"].presence }
      contract = find_existing_contract(attrs, contracting, winner_tax_ids) || Contract.new
      merge_contract_attributes!(contract, attrs, contracting)
      contract.save! if contract.new_record? || contract.changed?

      Array(attrs["winners"]).each do |winner_attrs|
        winner = find_or_create_entity_cached(
          winner_attrs["tax_identifier"],
          winner_attrs["name"],
          is_company: winner_attrs["is_company"] || false,
          cache: entity_cache
        )
        next unless winner

        ContractWinner.find_or_create_by!(contract: contract, entity: winner)
      end
    end

    # Cache-aware wrapper around find_or_create_entity. Keyed by
    # "#{tax_id}:#{country_code}" — unique per country scope.
    def find_or_create_entity_cached(tax_id, name, cache:, is_public_body: false, is_company: false)
      return nil if tax_id.blank? || name.blank?

      cache_key = "#{tax_id}:#{@ds.country_code}"
      cache[cache_key] ||= find_or_create_entity(tax_id, name, is_public_body: is_public_body, is_company: is_company)
    end

    def find_or_create_entity(tax_id, name, is_public_body: false, is_company: false)
      return nil if tax_id.blank? || name.blank?

      Entity.find_or_create_by!(tax_identifier: tax_id, country_code: @ds.country_code) do |e|
        e.name          = name
        e.is_public_body = is_public_body
        e.is_company    = is_company
      end
    end

    def normalize_contract_attrs(attrs)
      {
        "external_id" => attrs["external_id"].to_s.strip,
        "country_code" => normalize_country_code(attrs["country_code"]),
        "object" => attrs["object"].to_s.strip,
        "contract_type" => normalize_optional_text(attrs["contract_type"]),
        "procedure_type" => normalize_optional_text(attrs["procedure_type"]),
        "publication_date" => attrs["publication_date"],
        "celebration_date" => attrs["celebration_date"],
        "base_price" => attrs["base_price"],
        "total_effective_price" => attrs["total_effective_price"],
        "cpv_code" => normalize_optional_text(attrs["cpv_code"]),
        "location" => normalize_optional_text(attrs["location"]),
        "contracting_entity" => normalize_entity(attrs["contracting_entity"]),
        "winners" => Array(attrs["winners"]).map { |winner| normalize_entity(winner) }
      }
    end

    def normalize_entity(attrs)
      attrs ||= {}
      {
        "tax_identifier" => attrs["tax_identifier"].to_s.strip.presence,
        "name" => normalize_optional_text(attrs["name"]),
        "is_public_body" => attrs["is_public_body"] || false,
        "is_company" => attrs["is_company"] || false
      }
    end

    def normalize_country_code(country_code)
      (country_code.presence || @ds.country_code).to_s.upcase
    end

    def normalize_optional_text(value)
      value.is_a?(String) ? value.strip.presence : value
    end

    def merge_contract_attributes!(contract, attrs, contracting)
      CONTRACT_FIELDS.each do |attribute, key|
        merge_field!(contract, attribute, attrs[key])
      end

      if contract.new_record?
        contract.country_code = attrs["country_code"]
        contract.external_id  = attrs["external_id"]
      end
      contract.contracting_entity = contracting if contract.contracting_entity_id != contracting.id
      contract.data_source      ||= @ds
    end

    def merge_field!(contract, attribute, incoming_value)
      if contract.new_record?
        contract.public_send("#{attribute}=", incoming_value)
      elsif contract.public_send(attribute).blank? && incoming_value.present?
        contract.public_send("#{attribute}=", incoming_value)
      end
    end

    def find_existing_contract(attrs, contracting, winner_tax_ids)
      Contract.find_by(external_id: attrs["external_id"], country_code: attrs["country_code"]) ||
        find_contract_by_natural_key(attrs, contracting, winner_tax_ids)
    end

    # Secondary dedupe key for cross-source overlaps where external IDs differ.
    # We only attempt this when we have enough stable fields to avoid false matches.
    def find_contract_by_natural_key(attrs, contracting, winner_tax_ids)
      return nil if attrs["object"].blank? || attrs["base_price"].blank?

      date_field, date_value =
        if attrs["celebration_date"].present?
          [ :celebration_date, attrs["celebration_date"] ]
        elsif attrs["publication_date"].present?
          [ :publication_date, attrs["publication_date"] ]
        else
          return nil
        end

      scope = Contract.where(
        country_code: attrs["country_code"],
        contracting_entity_id: contracting.id,
        object: attrs["object"],
        base_price: attrs["base_price"]
      ).where(date_field => date_value)

      scope = scope.where(procedure_type: attrs["procedure_type"]) if attrs["procedure_type"].present?
      candidates = scope.includes(:winners).limit(5).to_a
      return nil if candidates.empty?

      if winner_tax_ids.present?
        normalized_incoming = winner_tax_ids.sort
        matches = candidates.select do |candidate|
          candidate.winners.pluck(:tax_identifier).sort == normalized_incoming
        end
        matches.one? ? matches.first : nil
      else
        candidates.one? ? candidates.first : nil
      end
    end
  end
end
