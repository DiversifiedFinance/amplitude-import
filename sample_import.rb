require 'rubygems'
require 'bundler/setup'
require 'logger'
require 'json'
require 'work_queue'
require 'retries'
require 'uri'
require 'time'
require 'net/http'
require 'net/https'

unless ENV['API_KEY']
  abort 'Must set API_KEY'
end

class AmplitudeImporter
  API_KEY = ENV['API_KEY'].freeze
  ENDPOINT = 'https://api.eu.amplitude.com/batch'.freeze

  def run(filename)
    submitted_count = 0
    logger.info "Processing #{filename}"
    uri = URI.parse(ENDPOINT)
    File.open(filename) do |f|
      f.each_line.lazy.each_slice(1000) do |lines|
        json_lines = lines.compact.map do |line|
          JSON.parse(line.strip).tap do |json|
            json['time'] = (Time.parse(json['event_time']).to_f * 1000).to_i
          end
        end

        queue.enqueue_b do
          with_retries(max_tries: 10) do
            body = { api_key: API_KEY,
                events: json_lines }.to_json
            response = Net::HTTP.post(
              uri,
              body,
              { 'Content-Type': 'application/json', 'Accept': '*/*' })
            if response.code == '200'
              logger.info "Response completed successfully"
            else
              msg = "Response failed with #{response.code}: #{response.message}"
              logger.info msg
              logger.info response.body
              raise msg
            end
          end
        end
        submitted_count += json_lines.size
        logger.info "Submitted batch of #{json_lines.size} events (#{submitted_count} total)"
      end
    end

    logger.info "Waiting for queue to finish..."
    queue.join

    logger.info "Found #{submitted_count} events for importing"
  end

  private

  def queue
    @queue ||= WorkQueue.new(100)
  end

  def logger
    @logger ||= Logger.new(STDERR)
  end
end

AmplitudeImporter.new.run(ARGV[0])
