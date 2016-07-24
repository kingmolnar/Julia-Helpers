#!/usr/bin/env julia

using DataFrames
SRCDIR=dirname(@__FILE__)

function floorplan(svgfile)
    fplan = open(`$SRCDIR/extractobjects.js $svgfile`) do io
        DataFrames.readtable(io)
    end

    fplan[:y] *= -1

    fplan[:xcenter] = fplan[:x] + fplan[:width]./2
    fplan[:ycenter] = fplan[:y] + fplan[:height]./2
    fplan[:objectid] = [ parse(UInt8, "0x"*fplan[k, :objectid]) for k in 1:size(fplan,1) ]

    fplan[:class] = [fplan[k, :color][1:5] for k in 1:nrow(fplan)]


    scales = fplan[ [match(r"^#40", fplan[k, :class])!=nothing for k in 1:nrow(fplan)], :]

    x_pix_ft = 1
    y_pix_ft = 1
    x_pix_meter = 1
    y_pix_meter = 1

    #feet2meter(x) = x./3.28084
    #meter2feet(x) = x.*3.28084

    if (s = findfirst(fplan[:class].=="#4000"); s>0)
        x_pix_ft = fplan[s, :width]/sqrt(fplan[s, :objectid])
        y_pix_ft = fplan[s, :height]/sqrt(fplan[s, :objectid])
        x_pix_meter = x_pix_ft*3.28084
        y_pix_meter = y_pix_ft*3.28084
    elseif (s = findfirst(fplan[:class].=="#4008"); s>0)
        x_pix_meter = fplan[s, :width]/sqrt(fplan[s, :objectid])
        y_pix_meter = fplan[s, :height]/sqrt(fplan[s, :objectid])
        x_pix_ft = x_pix_meter/3.28084
        y_pix_ft = y_pix_meter/3.28084
    elseif (sx = findfirst(fplan[:class].=="#4010"); sy = findfirst(fplan[:class].=="#4020"); sx>0 && sy>0)
        x_pix_ft = fplan[sx, :width]/fplan[sx, :objectid]
        y_pix_ft = fplan[sy, :height]/fplan[sy, :objectid]
        x_pix_meter = x_pix_ft*3.28084
        y_pix_meter = y_pix_ft*3.28084
    elseif (sx = findfirst(fplan[:class].=="#4018"); sy = findfirst(fplan[:class].=="#4028"); sx>0 && sy>0)
        x_pix_meter = fplan[sx, :width]/fplan[sx, :objectid]
        y_pix_meter = fplan[sy, :height]/fplan[sy, :objectid]
        x_pix_ft = x_pix_meter/3.28084
        y_pix_ft = y_pix_meter/3.28084
    else
        warn("Insufficent scale elements in floor plan")
    end

    if (f = findfirst(fplan[:class].=="#0000"); f>0)
        xframe = fplan[f, :x]
        yframe = fplan[f, :y]-fplan[f, :height]
        fplan[:x] -= xframe
        fplan[:xcenter] -= xframe
        fplan[:y] -= yframe
        fplan[:ycenter] -= yframe
    end

    for c in [:x, :width, :xcenter]
        fplan[symbol("$(c)_meter")] = fplan[c]./x_pix_meter
        fplan[symbol("$(c)_ft")] = fplan[c]./x_pix_ft
    end
    for c in [:y, :height, :ycenter]
        fplan[symbol("$(c)_meter")] = fplan[c]./y_pix_meter
        fplan[symbol("$(c)_ft")] = fplan[c]./y_pix_ft
    end

    fplan[:label] = ["$(uppercase(fplan[k, :object][1]))$(fplan[k, :objectid])" for k in 1:nrow(fplan)]
    # convert to metrix

    #sensors = fplan[fplan[:object].=="sensor", [:objectid, :xcenter, :ycenter]]
    #names!(sensors, [:objectid, :x, :y])

    #cameras = fplan[fplan[:object].=="camera", [:objectid, :xcenter, :ycenter]]
    #names!(cameras, [:objectid, :x, :y])

    #zones = fplan[fplan[:object].=="zone", [:objectid, :x, :y, :width, :height, :xcenter, :ycenter]]
    #names!(zones, [[:objectid, :xmin, :ymin, :width, :height, :xcenter, :ycenter]])
    #zones[:xmax] = zones[:xmin] + zones[:width]
    #zones[:ymax] = zones[:ymin] + zones[:height]
    #return sensors, cameras, zones, fplan

    return fplan
end

### use this with Gadfly to verify coordinates
###plot(fplan, x=:x_meter, y=:y_meter, color=:object, label=:label, Geom.point, Geom.label)

#=
if !isinteractive() && basename(@__FILE__)==basename(ENV["_"])
    #using Gadfly

    if length(ARGS)<1
        println("Usage: `basename(@__FILE__)` SVG-FILE [CSV-FILE]" )
        exit()
    end

    fplan = floorplan(ARGS[1])
    outfile = length(ARGS)>=2?ARGS[2]:"floorplan.csv"
    writetable(outfile, fplan)

    #draw(PDF("floorplan.pdf", 8inch, 8inch),
    #    plot(fplan[(fplan[:object].=="marker")|(fplan[:object].=="sensor")|(fplan[:object].=="camera"), :],
    #        x=:x_meter, y=:y_meter, color=:object, label=:label, Geom.point, Geom.label)
    #    )


    sensors = fplan[fplan[:object].=="sensor", :]
    for k in 1:nrow(sensors)
        println("\"index\":$(sensors[k, :objectid]), \"x\":$(sensors[k, :x_meter]), \"y\":$(sensors[k, :y_meter])")
    end

    locations = fplan[fplan[:object].=="marker", :]
    for k in 1:nrow(locations)
        println(",{\"label\":\"$(locations[k, :objectid])\", \"desc\":\"#$(locations[k, :objectid])\", \"x\":$(locations[k, :x_meter]), \"y\":$(locations[k, :y_meter])}")
    end
end
=#
