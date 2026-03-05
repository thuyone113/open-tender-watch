# frozen_string_literal: true

class EntitiesController < ApplicationController
  include ActionView::Helpers::NumberHelper

  PER_PAGE  = 50
  SORT_COLS = %w[celebration_date base_price object].freeze

  def index
    base = Entity.all

    if params[:q].present? && params[:q].length >= 2
      term = "%#{params[:q]}%"
      base = base.where("entities.name LIKE ? OR entities.tax_identifier LIKE ?", term, term)
    end

    base = base.where(is_public_body: true)  if params[:type] == "public"
    base = base.where(is_public_body: false) if params[:type] == "private"

    @total       = base.count
    @page        = [ params[:page].to_i, 1 ].max
    @total_pages = [ (@total.to_f / PER_PAGE).ceil, 1 ].max

    @entities = base
      .left_outer_joins(:contracts_as_contracting_entity)
      .select("entities.*, COUNT(contracts.id) AS contract_count")
      .group("entities.id")
      .order("contract_count DESC, entities.name ASC")
      .limit(PER_PAGE)
      .offset((@page - 1) * PER_PAGE)
  end

  def show
    @entity = Entity.find(params[:id])

    base_scope = @entity.contracts_as_contracting_entity
                        .includes(:winners, :data_source, :flags)

    @sort_col = SORT_COLS.include?(params[:sort]) ? params[:sort] : "celebration_date"
    @sort_dir = params[:dir] == "asc" ? "asc" : "desc"

    @total       = base_scope.count
    @page        = [ params[:page].to_i, 1 ].max
    @total_pages = (@total.to_f / PER_PAGE).ceil

    @contracts = base_scope
      .order(Arel.sql("#{@sort_col} #{@sort_dir}, id #{@sort_dir}"))
      .limit(PER_PAGE)
      .offset((@page - 1) * PER_PAGE)

    # Aggregate flag exposure for this entity from the pre-computed table
    @flag_stats = FlagEntityStat
      .where(entity_id: @entity.id)
      .group(:flag_type)
      .select(
        "flag_type",
        "SUM(total_exposure)  AS total_exposure",
        "SUM(contract_count)  AS contract_count",
        "MAX(severity)        AS severity"
      )
      .order("total_exposure DESC")
  end
end
