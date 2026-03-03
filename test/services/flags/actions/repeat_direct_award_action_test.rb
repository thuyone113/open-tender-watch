require "test_helper"

class Flags::Actions::RepeatDirectAwardActionTest < ActiveSupport::TestCase
  def create_supplier
    Entity.create!(
      name: "Fornecedor Teste Lda",
      tax_identifier: "509#{rand(100_000..999_999)}",
      country_code: "PT",
      is_public_body: false,
      is_company: true
    )
  end

  def create_direct_award(external_id:, authority:, supplier:, publication_date:)
    contract = Contract.create!(
      external_id: external_id,
      country_code: "PT",
      object: "Contrato #{external_id}",
      procedure_type: "Ajuste Direto",
      base_price: 5_000,
      publication_date: publication_date,
      contracting_entity: authority,
      data_source: data_sources(:portal_base)
    )
    ContractWinner.create!(contract: contract, entity: supplier)
    contract
  end

  test "flags all contracts when same authority+supplier has 3+ direct awards in 36 months" do
    authority = entities(:one)
    supplier  = create_supplier

    c1 = create_direct_award(external_id: "a1-1", authority: authority, supplier: supplier, publication_date: Date.new(2024, 1, 1))
    c2 = create_direct_award(external_id: "a1-2", authority: authority, supplier: supplier, publication_date: Date.new(2024, 6, 1))
    c3 = create_direct_award(external_id: "a1-3", authority: authority, supplier: supplier, publication_date: Date.new(2024, 12, 1))

    assert_difference "Flag.count", 3 do
      result = Flags::Actions::RepeatDirectAwardAction.new.call
      assert_equal 3, result
    end

    [ c1, c2, c3 ].each do |contract|
      flag = Flag.find_by!(contract_id: contract.id, flag_type: "A1_REPEAT_DIRECT_AWARD")
      assert_equal "high", flag.severity
      assert_equal 3, flag.details["award_count"]
    end
  end

  test "does not fire when there are only 2 direct awards between the same pair" do
    authority = entities(:one)
    supplier  = create_supplier

    create_direct_award(external_id: "a1-pair-1", authority: authority, supplier: supplier, publication_date: Date.new(2024, 1, 1))
    create_direct_award(external_id: "a1-pair-2", authority: authority, supplier: supplier, publication_date: Date.new(2024, 6, 1))

    assert_no_difference "Flag.count" do
      result = Flags::Actions::RepeatDirectAwardAction.new.call
      assert_equal 0, result
    end
  end

  test "does not fire when 3+ awards span more than 36 months" do
    authority = entities(:one)
    supplier  = create_supplier

    create_direct_award(external_id: "a1-span-1", authority: authority, supplier: supplier, publication_date: Date.new(2021, 1, 1))
    create_direct_award(external_id: "a1-span-2", authority: authority, supplier: supplier, publication_date: Date.new(2022, 6, 1))
    create_direct_award(external_id: "a1-span-3", authority: authority, supplier: supplier, publication_date: Date.new(2024, 3, 1))

    assert_no_difference "Flag.count" do
      Flags::Actions::RepeatDirectAwardAction.new.call
    end
  end

  test "does not fire for non-direct-award procedure types" do
    authority = entities(:one)
    supplier  = create_supplier

    3.times do |i|
      contract = Contract.create!(
        external_id: "a1-tender-#{i}",
        country_code: "PT",
        object: "Concurso #{i}",
        procedure_type: "Concurso público",
        base_price: 5_000,
        publication_date: Date.new(2024, i + 1, 1),
        contracting_entity: authority,
        data_source: data_sources(:portal_base)
      )
      ContractWinner.create!(contract: contract, entity: supplier)
    end

    assert_no_difference "Flag.count" do
      Flags::Actions::RepeatDirectAwardAction.new.call
    end
  end

  test "does not fire when contracts have no publication_date" do
    authority = entities(:one)
    supplier  = create_supplier

    3.times do |i|
      contract = Contract.create!(
        external_id: "a1-nodate-#{i}",
        country_code: "PT",
        object: "Sem data #{i}",
        procedure_type: "Ajuste Direto",
        base_price: 5_000,
        publication_date: nil,
        contracting_entity: authority,
        data_source: data_sources(:portal_base)
      )
      ContractWinner.create!(contract: contract, entity: supplier)
    end

    assert_no_difference "Flag.count" do
      Flags::Actions::RepeatDirectAwardAction.new.call
    end
  end

  test "separate authority+supplier pairs are evaluated independently" do
    authority  = entities(:one)
    supplier_a = create_supplier
    supplier_b = create_supplier

    # Supplier A: only 2 awards → no flag
    create_direct_award(external_id: "a1-sep-a1", authority: authority, supplier: supplier_a, publication_date: Date.new(2024, 1, 1))
    create_direct_award(external_id: "a1-sep-a2", authority: authority, supplier: supplier_a, publication_date: Date.new(2024, 6, 1))

    # Supplier B: 3 awards → flagged
    b1 = create_direct_award(external_id: "a1-sep-b1", authority: authority, supplier: supplier_b, publication_date: Date.new(2024, 1, 1))
    b2 = create_direct_award(external_id: "a1-sep-b2", authority: authority, supplier: supplier_b, publication_date: Date.new(2024, 6, 1))
    b3 = create_direct_award(external_id: "a1-sep-b3", authority: authority, supplier: supplier_b, publication_date: Date.new(2024, 9, 1))

    assert_difference "Flag.count", 3 do
      result = Flags::Actions::RepeatDirectAwardAction.new.call
      assert_equal 3, result
    end

    assert_not Flag.exists?(contract: b1, flag_type: "A1_REPEAT_DIRECT_AWARD") == false
    [ b1, b2, b3 ].each do |c|
      assert Flag.exists?(contract_id: c.id, flag_type: "A1_REPEAT_DIRECT_AWARD")
    end
  end

  test "is idempotent" do
    authority = entities(:one)
    supplier  = create_supplier

    3.times do |i|
      create_direct_award(external_id: "a1-idem-#{i}", authority: authority, supplier: supplier, publication_date: Date.new(2024, i + 1, 1))
    end

    action = Flags::Actions::RepeatDirectAwardAction.new
    assert_equal 3, action.call
    assert_no_difference "Flag.count" do
      assert_equal 3, action.call
    end
  end

  test "removes stale flags when a contract is removed from the pattern" do
    authority = entities(:one)
    supplier  = create_supplier

    c1 = create_direct_award(external_id: "a1-stale-1", authority: authority, supplier: supplier, publication_date: Date.new(2024, 1, 1))
    c2 = create_direct_award(external_id: "a1-stale-2", authority: authority, supplier: supplier, publication_date: Date.new(2024, 4, 1))
    c3 = create_direct_award(external_id: "a1-stale-3", authority: authority, supplier: supplier, publication_date: Date.new(2024, 8, 1))

    action = Flags::Actions::RepeatDirectAwardAction.new
    assert_equal 3, action.call

    # Move c3 outside the 36-month window so pattern no longer qualifies
    c3.update!(publication_date: Date.new(2027, 6, 1))

    assert_equal 0, action.call
    assert_equal 0, Flag.where(flag_type: "A1_REPEAT_DIRECT_AWARD").count
  end
end
