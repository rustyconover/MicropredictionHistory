module MicropredictionHistory

using SHA
using Base64
using GZip
using Microprediction
using StatsBase
using Dates
using TimeSeries

function readData(filename)
	try
        stream = GZip.open(filename, "r")
        data = reshape(ntoh.(reinterpret(Float64, read(stream))), (2, :))
        return data
    catch
		@warn "Could not read file $(filename)"
	end
end

function readDirectory(directory)
	result = nothing
	for file in readdir(directory, join=true)
		data = readData(file)
		data[1, :] = reverse(data[1, :])
		data[2, :] = reverse(data[2, :])

		if result === nothing
			result = data
		else
			result = cat(result, data, dims=2)
		end
	end
	return result
end

struct LoadedStream
    data::TimeArray
    update_interval::Second
end

export LoadStream, LoadedStream

"""
    loadStream()

Load the contents of a stream along with its full history.  Optionally 
load the live values of the stream as well.

"""
function loadStream(
    data_folder::AbstractString,
    stream_name::AbstractString;
    load_live_data::Bool=false)::LoadedStream
	encoded_stream_name = replace(base64encode(sha256(stream_name)), r"\/" => "_")
	directory = Base.Filesystem.joinpath(data_folder, encoded_stream_name)

    all_data = unique(readDirectory(directory), dims=2)

    # Put the data in sorted order, irregardless of how it has been serialized.
    all_data = all_data[:, sortperm(all_data[1, :])]

    stream_timestamps = all_data[1, :]
    stream_values = all_data[2, :]

    # To determine how often the series is updating, round off the mode 
    # of the series to the closest minute
    series_update_frequency = round(Dates.Second(mode(round.(diff(stream_timestamps)))), Dates.Second(60))

    # Since data values aren't received precisely at the series_update_frequency
    # round the timestamps of the received rows to the closest interval.
    rounded_timestamps = round.(Dates.unix2datetime.(stream_timestamps), series_update_frequency)

    data = (datetime = rounded_timestamps,
            Count = stream_values)
	ta = TimeArray(data; timestamp = :datetime)

	# The TimeArray may be missing values because the timestamps are the truncated ones from the 
	# recorded stream history.
    
    if load_live_data 
        read_config = Microprediction.Config()
        live_lagged_values = Microprediction.get_lagged(read_config, stream_name)
        # Since the live values may not be cleaned up and truncated, we need to do that.
        live_lagged_values = TimeArray(round.(timestamp(live_lagged_values), series_update_frequency), values(live_lagged_values), ["Count"])

        # println("Historic date range")
        # println("$(timestamp(ta[1])[1]) - $(timestamp(ta[end])[1])")

        # println("Live date range")
        # println("$(timestamp(live_lagged_values[1])[1]) - $(timestamp(live_lagged_values[end])[1])")

        # If you vcat() a TimeArray and there are rows for the same timestamp
        # the code appears to remove those rows, so trim the live data such that it starts
        # once the historic data end.
        live_lagged_values = from(live_lagged_values, timestamp(ta[end])[1] + Dates.Minute(1))
        # println("Trimmed Live date range")
        # println("$(timestamp(live_lagged_values[1])[1]) - $(timestamp(live_lagged_values[end])[1])")

        # FIXME: in the future, make sure that there aren't time intervals that are missing 
	    # between the historic and live lagged values.
        ta = vcat(ta, live_lagged_values)
    end

	
	# Make a StepRange for each minute, that should exist.
	all_dates = timestamp(ta[1])[1]:series_update_frequency:timestamp(ta[end])[1]

	# Create a function that will either get the value from the TimeArray or it will return missing.
	function getValue(s)
		return function foo(date)
			v = values(s[date])
			if v === nothing
				missing
    		else
				v[1]
			end	
		end
	end
	
    all_values = map(getValue(ta), all_dates)
    raw_frame = TimeArray(all_dates, all_values)
    raw_frame = from(raw_frame, DateTime(2020, 9, 11, 0, 0, 0))

    return LoadedStream(raw_frame, series_update_frequency)
end


end # module
