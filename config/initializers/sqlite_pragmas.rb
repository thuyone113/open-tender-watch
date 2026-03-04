# Ensure every SQLite connection gets a generous busy_timeout so that a
# long-running import doesn't get killed by momentary lock contention.
# This supplements the timeout: key in database.yml (which the adapter also
# uses) in case of edge cases where the pragma isn't applied on reconnect.
if defined?(ActiveRecord::ConnectionAdapters::SQLite3Adapter)
  ActiveSupport.on_load(:active_record) do
    ActiveRecord::ConnectionAdapters::SQLite3Adapter.class_eval do
      prepend(Module.new do
        def configure_connection
          super
          @raw_connection.execute("PRAGMA journal_mode=WAL")
          @raw_connection.execute("PRAGMA synchronous=NORMAL")
          @raw_connection.execute("PRAGMA busy_timeout=30000")
        end
      end)
    end
  end
end
