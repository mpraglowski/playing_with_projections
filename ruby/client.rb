require 'json'
require 'rest-client'

def read_from_uri stream_id
  stream = "https://playing-with-projections.herokuapp.com/stream/#{stream_id}"
  # stream = "https://raw.githubusercontent.com/tcoopman/playing_with_projections_server/master/data/#{stream_id}.json"
  puts "Reading from '#{stream}'"
  RestClient.get stream
end

def read_from_file file_path
  puts "Reading from '#{file_path}'"
  File.read(file_path)
end

def transform_date event
  event['timestamp'] = DateTime.parse(event['timestamp'])
  event
end

stream = ARGV.first || '../data/0.json'

# raw_data = read_from_uri(stream)
raw_data = read_from_file(stream)

def type_of(e)
  e['type']
end

def payload(e)
  e['payload']
end

def data(e, attribute)
  payload(e)[attribute.to_s]
end

def timestamp(e)
  e['timestamp']
end

def events_of(events, type)
  events.select{|e| type_of(e) == type}
end

def events_for(events, start, finish)
  events.select{|e| t = timestamp(e); t >= start && t < finish }
end

events = JSON.parse(raw_data).map(&method(:transform_date))
puts "Number of events: #{events.count}"

# 0. types of events count
puts events.inject({}) {|acc, e| type = e['type']; acc[type] = acc.fetch(type,0)+1; acc}

# 1. playeys registered
puts events.inject(0) {|acc, e| acc +=1 if e['type'] == 'PlayerHasRegistered'; acc}

# 1b. playeys registered per month
puts events.inject(Hash.new(0)) {|acc, e| acc[timestamp(e).month] +=1 if type_of(e) == 'PlayerHasRegistered'; acc}

# 2. top 10 of most played quizes
started_games = events.inject([]) {|acc,e| acc << data(e, :game_id) if type_of(e) == 'GameWasStarted'; acc}
quizes = events.inject({}) {|acc,e| acc[data(e, :quiz_id)] = data(e, :quiz_title) if type_of(e) == 'QuizWasCreated'; acc}
puts events.inject(Hash.new(0)) {|acc,e| acc[data(e, :quiz_id)] += 1 if type_of(e) == 'GameWasOpened' && started_games.include?(data(e, :game_id)); acc}
  .sort_by{|k,v| v}.last(10).reverse.map{|x| {quiz: quizes[x[0]], count: x[1]}}

# 2b. top 10 of most played quizes in june 2016
puts events_for(events, Date.new(2016,06,01),Date.new(2016,07,01))
  .inject(Hash.new(0)) {|acc,e| acc[data(e, :quiz_id)] += 1 if type_of(e) == 'GameWasOpened' && started_games.include?(data(e, :game_id)); acc}
  .sort_by{|k,v| v}.last(10).reverse.map{|x| {quiz: quizes[x[0]], count: x[1]}}

# 3. Active players = player > 5 games in a month
#    Who are the active players & how many in 06/2016
players = events_of(events, 'PlayerHasRegistered').inject({}) {|acc,e| acc[data(e, :player_id)] = "#{data(e, :first_name)} #{data(e, :last_name)}"; acc}
active_players = events_of(events, 'PlayerJoinedGame').inject(Hash.new(0)) {|acc,e| acc[data(e,:player_id)] += 1; acc}
  .select{|player_id, count| count > 5}.map{|id,count| id}
puts active_players.map{|id| {id: id, name: players[id]}}

puts events_for(events_of(events, 'PlayerJoinedGame'), Date.new(2016,06,01),Date.new(2016,07,01))
  .inject(Hash.new(0)) {|acc,e| acc[data(e, :player_id)] += 1; acc}
  .map{|player_id, count| {id: player_id, name: players[player_id], count: count} if active_players.include?(player_id)}
  .compact

# 4. verify the marketing campaign
#    players registration / joining the game should increase month to month
#    and played many games

def prev_month(date)
  return Date.new(date.year - 1, 12, 1) if date.month == 1
  Date.new(date.year, date.month - 1, 1)
end

registration_per_month = events_of(events, 'PlayerHasRegistered')
  .inject(Hash.new(0)) {|acc,e| t = timestamp(e); acc[Date.new(t.year, t.month, 1)] += 1; acc}
registration_delta_per_month = registration_per_month.inject({}) do |acc, registration|
  month, count = registration
  acc[month] = count - (registration_per_month[prev_month(month)] || 0)
  acc
end
puts "Registrations (delta)"
puts registration_delta_per_month
  .map{|m,c| "#{m.year}-#{m.month}: #{c}" }
  .inspect

joining_the_game_per_month = events_of(events, 'PlayerJoinedGame')
  .inject(Hash.new(0)) {|acc,e| t = timestamp(e); acc[Date.new(t.year, t.month, 1)] += 1; acc}
joining_the_game_delta_per_month = joining_the_game_per_month.inject({}) do |acc, joins|
  month, count = joins
  acc[month] = count - (joining_the_game_per_month[prev_month(month)] || 0)
  acc
end
puts "Joining the game (delta)"
puts joining_the_game_delta_per_month
  .map{|m,c| "#{m.year}-#{m.month}: #{c}" }
  .inspect

# TBC
