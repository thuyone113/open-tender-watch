module ApplicationHelper
  # Translates a raw FLAG_TYPE constant (e.g. "A2_PUBLICATION_AFTER_CELEBRATION")
  # into a human-readable localised name. Falls back to a humanised version of
  # the constant when no translation key exists.
  def flag_type_label(flag_type)
    key = "flags.types.#{flag_type.to_s.downcase}"
    t(key, default: flag_type.to_s.tr("_", " ").squeeze(" ").strip.titleize)
  end
end
