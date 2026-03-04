# frozen_string_literal: true

namespace :import do
  desc "Import one page from every active DataSource (fast smoke-test)"
  task once: :environment do
    DataSource.active.each do |ds|
      puts "  #{ds.adapter_class}..."
      PublicContracts::ImportService.new(ds).call
      puts "    #{ds.reload.record_count} records, status: #{ds.status}"
    end
  end

  desc "Paginate and import ALL records from a specific adapter (or all if omitted)"
  task :all, [ :adapter ] => :environment do |_, args|
    sources = if args[:adapter].present?
      DataSource.where(adapter_class: args[:adapter])
    else
      DataSource.active
    end

    sources.each do |ds|
      puts "\n[#{ds.adapter_class}]"
      PublicContracts::ImportService.new(ds).call_all
      puts "  status: #{ds.reload.status}, total: #{ds.record_count}"
    end
  end

  desc "Import all SNS health-sector contracts (~43K records)"
  task sns: :environment do
    ds = DataSource.find_by!(adapter_class: "PublicContracts::PT::SnsClient")
    puts "Starting SNS full import. Total available: #{ds.adapter.total_count}"
    PublicContracts::ImportService.new(ds).call_all
    puts "Finished. Contracts in DB: #{Contract.where(data_source: ds).count}"
  end

  desc "Import all TED EU procurement notices for the configured country"
  task ted: :environment do
    ds = DataSource.find_by!(adapter_class: "PublicContracts::EU::TedClient")
    puts "Starting TED full import. Total available: #{ds.adapter.total_count}"
    PublicContracts::ImportService.new(ds).call_all
    puts "Finished. Contracts in DB: #{Contract.where(data_source: ds).count}"
  end

  desc "Import all QuemFatura.pt contracts (~23K records). Requires cf_clearance in DataSource config."
  task quem_fatura: :environment do
    ds = DataSource.find_by!(adapter_class: "PublicContracts::PT::QuemFaturaClient")
    puts "Starting QuemFatura.pt full import. Total available: #{ds.adapter.total_count}"
    PublicContracts::ImportService.new(ds).call_all
    puts "Finished. Contracts in DB: #{Contract.where(data_source: ds).count}"
  end

  desc "Import Portal BASE contracts from dados.gov.pt XLSX (current year by default)"
  task portal_base: :environment do
    ds      = DataSource.find_by!(adapter_class: "PublicContracts::PT::PortalBaseClient")
    adapter = ds.adapter
    years   = adapter.instance_variable_get(:@years)

    puts "Starting Portal BASE import (year(s): #{years.join(', ')})..."
    puts "Querying dados.gov.pt for XLSX resources..."

    estimated = adapter.total_count
    puts "  Estimated rows: ~#{estimated.to_s.reverse.scan(/.{1,3}/).join(',').reverse}"
    puts "  Mode: #{adapter.respond_to?(:each_contract) ? 'streaming (single-pass, entity-cached)' : 'paginated'}"

    t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    PublicContracts::ImportService.new(ds).call_all
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0

    count = Contract.where(data_source: ds).count
    puts "Finished in #{elapsed.round(1)}s. Contracts in DB: #{count.to_s.reverse.scan(/.{1,3}/).join(',').reverse}"
  end
end
