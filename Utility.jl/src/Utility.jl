module Utility
using DataFrames
import GZip
export jp!, jp, LogMessage, AbstractLogger, TextLogger, CSVLogger, JSONLogger, SerializeLogger
export AbstractLogFile, SerializeLogFile


function jp!(args...; keep=true)
    fn = joinpath(args...)
    bn = basename(fn)
    dn = dirname(fn)
    if !ispath(dn)
        mkpath(dn)
    else
        @assert isdir(dn) "Error: `$dn` exists but is not a directory!"
    end
    s = stat(fn)
    if keep && s.size>0 ### file exists
        ts = unix2datetime(s.mtime)
        ###nfn = "$fn~$(replace("$ts", ":", "_"))"
        nfn = "$fn~$ts"
        run(`mv $fn $nfn`)
    end
    fn
end

jp = joinpath

if VERSION < v"0.4-dev"
    using Dates
else
    using Base.Dates
end


immutable LogMessage
    destination::AbstractString
    message         ###::AbstractString
    TS::DateTime
    LogMessage(des::AbstractString, msg) = new(des, msg, now())
    LogMessage(msg) = new("stdlog", msg, now())
end

function logfile_open(file::AbstractString, append::Bool=true)
    if ismatch(r"\.gz\w*", basename(file)) || ismatch(r"\.\w*gz$", basename(file))
        GZip.open(file, append?"a":"w")
    else
        open(file, append?"a":"w")
    end
end

function logfile_open(fct::Function, file::AbstractString, append::Bool=true)
    if ismatch(r"\.gz\w*", basename(file)) || ismatch(r"\.\w*gz$", basename(file))
        GZip.open(fct, file, append?"a":"w")
    else
        open(fct, file, append?"a":"w")
    end
end


abstract AbstractLogger


immutable TextLogger <: AbstractLogger
    channel::Channel
    task::Task
    function TextLogger(file::AbstractString, n=1_000; append=true)
        cha = Channel{LogMessage}(n)
        if !isdir(dirname(file))
            mkpath(dirname(file))
        end
        info("Open log file `$file'")
        tsk = @schedule logfile_open(file, append) do io ### open(file, append?"a":"w") do io
            for l in cha
                write(io, "$(l.destination)\t$(l.TS)\t$(l.message)\n")
                flush(io)
            end
            info("Close log file `$file'")
        end
        new(cha,tsk)
    end
    function TextLogger(io, n=1_000)
        cha = Channel{LogMessage}(n)
        tsk = @schedule begin
            for l in cha
                write(io, "$(l.destination)\t$(l.TS)\t$(l.message)\n")
                flush(io)
            end
        end
        new(cha,tsk)
    end
end

immutable CSVLogger <: AbstractLogger
    channel::Channel
    task::Task

    _show(io, u::Unsigned) = write(io, "$u")
    function _show{T}(io, arr::AbstractArray{T,1}; seperator=",")
        _show(io, arr[1])
        for i in 2:length(arr)
            write(io, seperator)
            _show(io, arr[i])
        end
    end
    _show(io, x) = show(io, x)

    function fieldnames2header(tp)
        fns = fieldnames(tp)
        h = []
        for f in fns
            if isa(getfield(tp, f), AbstractArray)
                for i in 1:length(getfield(tp, f))
                    push!(h, "$(f)_$i")
                end
            else
                push!(h, "$f")
            end
        end
        h
    end

    function CSVLogger(file::AbstractString, n=1_000; append=true, seperator=",")
        cha = Channel{LogMessage}(n)
        if !isdir(dirname(file))
            mkpath(dirname(file))
        end

        ios = Dict()
        current_types = Dict()
        tsk = @schedule begin
            ###logfile_open(file, append) do io ### open(file, append?"a":"w") do io
            for l in cha
                ## need to open/create file?
                if l.destination ∉ keys(ios)
                    info("Open log file `$(jp(file,"$(l.destination).csv"))'")
                    ios[l.destination] = append ?
                        logfile_open(jp(file,"$(l.destination).csv"), true) :
                        logfile_open(jp!(file,"$(l.destination).csv"), false)

                    current_types[l.destination] = nothing
                end

                io = ios[l.destination]
                if typeof(l.message) <: AbstractString
                    write(io, "# $(l.message)\n")
                else
                    if current_types[l.destination] != typeof(l.message)
                        current_types[l.destination] = typeof(l.message)
                        write(io, "$(join(fieldnames2header(l.message), seperator))$(seperator)__TimeStamp\n")
                    end
                    fns = fieldnames(current_types[l.destination])
                    ## first column
                    if isa(getfield(l.message, fns[1]), AbstractArray)
                        _show(io, getfield(l.message, fns[1])[1])
                        for i in 2:length(getfield(l.message, fns[1]))
                            write(io, seperator)
                            _show(io, getfield(l.message, fns[1])[i])
                        end
                    else
                        _show(io, getfield(l.message, fns[1]))
                    end

                    ## the rest of the columns
                    for n in fns[2:end]
                        ##gf = getfield(l.message, n)
                        if isa(getfield(l.message, n), AbstractArray)
                            for i in 1:length(getfield(l.message, n))
                                write(io, seperator)
                                _show(io, getfield(l.message, n)[i])
                            end
                        else
                            write(io, seperator)
                            _show(io, getfield(l.message, n))
                        end
                    end
                    write(io, "$(seperator)$(l.TS)\n")
                end
                flush(io)
            end
            info("Close log files in `$file'")
            for i in values(ios)
                close(i)
            end
        end
        new(cha,tsk)
    end
end


immutable SerializeLogger <: AbstractLogger
    channel::Channel
    task::Task
    function SerializeLogger(file::AbstractString, n=1_000; append=true)
        cha = Channel{LogMessage}(n)
        if !isdir(dirname(file))
            mkpath(dirname(file))
        end
        info("Open log file `$file'")
        tsk = @schedule logfile_open(file, append) do io
            for l in cha
                serialize(io, l) ##$$"$(l.destination)\t$(l.TS)\t$(l.message)\n")
                flush(io)
            end
            info("Close log file `$file'")
        end
        new(cha,tsk)
    end
    function SerializeLogger(io, n=1_000)
        cha = Channel{LogMessage}(n)
        tsk = @schedule begin
            for l in cha
                serialize(io, "$(l.destination)\t$(l.TS)\t$(l.message)\n")
                flush(io)
            end
        end
        new(cha,tsk)
    end
end




using JSON

function safejson(obj)
    try
        json(obj)
    catch
        Dict(:o=>"$(typeof(obj))", :d=>"$(obj)")
    end
end

immutable JSONLogger <: AbstractLogger
    channel::Channel
    task::Task

    function JSONLogger(file::AbstractString, n=1_000; includetype=false)
        cha = Channel{LogMessage}(n)
        if !isdir(dirname(file))
            mkpath(dirname(file))
        end
        info("Open JSON file `$file'")
        if includetype
            tsk = @schedule open(file, "a") do io
                for l in cha
                    write(io, JSON.json(Dict(:type=>"log",
                                             :data=>Dict(:dest=>l.destination, :TS=>l.TS, :msg=>safejson(l.message))))
                          )
                    write(io, "\n")
                    flush(io)
                end
                info("Close JSON file `$file'")
            end
        else
            tsk = @schedule open(file, "a") do io
                for l in cha
                    write(io, JSON.json(Dict(:dest=>l.destination, :TS=>l.TS, :msg=>safejson(l.message))))
                    write(io, "\n")
                    flush(io)
                end
                info("Close log file `$file'")
            end
        end
        new(cha,tsk)
    end

    function JSONLogger(io, n=1_000; includetype=false)
        cha = Channel{LogMessage}(n)
        if includetype
            tsk = @schedule begin
                for l in cha
                    write(io, JSON.json(Dict(:type=>"log",
                                             :data=>Dict(:dest=>l.destination, :TS=>l.TS, :msg=>safejson(l.message))))
                          )
                    write(io, "\n")
                    flush(io)
                end
            end
        else
            tsk = @schedule begin
                for l in cha
                    write(io, JSON.json(Dict(:dest=>l.destination, :TS=>l.TS, :msg=>safejson(l.message))))
                    write(io, "\n")
                    flush(io)
                end
            end
        end
        new(cha,tsk)
    end
end

import Base.put!
function put!(logger::AbstractLogger, msg::LogMessage)
    put!(logger.channel, msg)
end


function put!(logger::AbstractLogger, dest::AbstractString, msg)
    put!(logger.channel, LogMessage(dest, msg))
end

function put!(logger::AbstractLogger, msg)
    put!(logger.channel, LogMessage(msg))
end
export put!


abstract AbstractLogFile
Base.start(::AbstractLogFile) = 1
Base.eltype(::Type{AbstractLogFile}) = LogMessage
Base.length(::AbstractLogFile) = 0

immutable TextLogFile <: AbstractLogFile
    io
    function TextLogFile(file::AbstractString)
        new(logfile_open(file))
    end
end
function Base.next(lf::TextLogFile, state)
    ##write(io, "$(l.destination)\t$(l.TS)\t$(l.message)\n")
    lin = split(readline(lf.io)[1:end-1], "\t")
    LogMessage(lin[1], lin[2], DateTime(lin[3])), state+1
end
function Base.done(lf::TextLogFile, state)
    if eof(lf.io)
        close(lf.io)
        return true
    else
        return false
    end
end


immutable SerializeLogFile <: AbstractLogFile
    io
    function SerializeLogFile(file::AbstractString)
        new(logfile_open(file))
    end
end
Base.next(lf::SerializeLogFile, state) = (deserialize(lf.io), state+1)
function Base.done(lf::SerializeLogFile, state)
    if eof(lf.io)
        close(lf.io)
        return true
    else
        return false
    end
end



include("dataframeutils.jl")


#       _____  _      _      ____  _     _           _         _          __  __
#      |  __ \(_)    | |    / __ \| |   (_)         | |       | |        / _|/ _|
#      | |  | |_  ___| |_  | |  | | |__  _  ___  ___| |_   ___| |_ _   _| |_| |_
#      | |  | | |/ __| __| | |  | | '_ \| |/ _ \/ __| __| / __| __| | | |  _|  _|
#      | |__| | | (__| |_  | |__| | |_) | |  __/ (__| |_  \__ \ |_| |_| | | | |
#      |_____/|_|\___|\__|  \____/|_.__/| |\___|\___|\__| |___/\__|\__,_|_| |_|
#                                      _/ |
#                                     |__/

function dict2args(T::Type, dict::Dict)

    function pushzero!(x, t::Type)
        if t<:Real
            push!(x, zero(t))
        elseif t<:AbstractString
            push!(x, "")
        else
            push!(x, nothing)
        end
    end

    function _np(t::Type, v)
        if t==UInt8
            s = "$v"
            if s[1]=='f' || s[1]=='F'
                UInt8(0)
            elseif s[1]=='t' || s[1]=='T'
                UInt8(1)
            else
                parse(UInt8, s)
            end
        elseif t<:Real
            parse(t, "$v")
        else
            try
                t(v)
            catch
                v
            end
        end
    end


    x = []
    for n in fieldnames(T)
        t = fieldtype(T, n)
        if t<:AbstractDataArray
            if haskey(dict, "$n")
                push!(x, _np(t, dict["$n"]))
            else
                rr = Regex("^$(n)_(\d+)")
                l = sort([ (m=match(Regex("^$(n)_(\\d+)"), "$k"); m!=nothing?parse(Int, m[1]):0) for k in keys(dict) ])
                da = DataArray(eltype(t), 0)
                for i in l[l.>0]
                    if dict["$(n)_$i"]=="NA"
                        da = vcat(da, NA)
                    else
                        da = vcat(da, _np(eltype(t), dict["$(n)_$i"]))
                    end
                end
                push!(x, da)
            end
        elseif t<:AbstractArray
            if haskey(dict, "$n")
                push!(x, _np(t, dict["$n"]))
            else
                rr = Regex("^$(n)_(\d+)")
                l = sort([ (m=match(Regex("^$(n)_(\\d+)"), "$k"); m!=nothing?parse(Int, m[1]):0) for k in keys(dict) ])
                da = eltype(t)[ _np(eltype(t), dict["$(n)_$i"]) for i in l[l.>0]]
                push!(x, da)
            end
        elseif haskey(dict, "$n")
            push!(x, _np(t, dict["$n"]))
        else
            pushzero!(x, t)
        end
    end
    x
end



#       ______ _                        _
#      |  ____| |                      | |
#      | |__  | | ___   ___  _ __ _ __ | | __ _ _ __
#      |  __| | |/ _ \ / _ \| '__| '_ \| |/ _` | '_ \
#      | |    | | (_) | (_) | |  | |_) | | (_| | | | |
#      |_|    |_|\___/ \___/|_|  | .__/|_|\__,_|_| |_|
#                                | |
#                                |_|

export floorplan
include("floorplan.jl")

end # module Utility
