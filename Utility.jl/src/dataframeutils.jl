export pivot, stats, summarystats, dropna, countna, countnan, seteltype!, maprow
export longformat, wideformat
export lattice, matrixtodataframe

function pivot(df::AbstractDataFrame, row::Symbol, col::Symbol, val::Symbol, fct::Function; rmna=true)
    if rmna
        res = by(df[!isna(df[val]),[row, col, val]], [row, col], f -> DataFrame(pivot_value = fct(f[val])))
    else
        res = by(df[[row, col, val]], [row, col], f -> DataFrame(pivot_value = fct(f[val])))
    end
    sort!(res, cols=[row, col])
    colcol = symbol(string(col)*"__colum_names")
    res[colcol] = ["$(string(col))_$(res[k, col])" for k in 1:size(res,1)]
    pv = unstack(res, row, colcol, :pivot_value)
    pvnams = names(pv)
    pvnams[1] = row
    names!(pv, pvnams)
    pv
end

function pivot(df::AbstractDataFrame, row::Symbol, col::Symbol, vals::Array{Symbol,1}, fct::Function; rmna=true)
    if rmna
        ddf = dropna(df, vals)
    else
        ddf = df
    end
    pvt = pivot(ddf, row, col, vals[1], fct, rmna=false)
    for v in 2:length(vals)
        p = pivot(ddf, row, col, vals[v], fct, rmna=false)

        pvt = join(pvt, p , on=row)
    end
    ##println(names(pvt))
    colsymbs = sort(levels(ddf[col]))
    nam = [row]
    for vv in vals, cc in sort(levels(ddf[col]))
        nam = vcat(nam, symbol("$(string(col))_$(cc)_$(vv)"))
    end
    ##println(nam)
    names!(pvt, nam)
    pvt
end

function pivot(df::AbstractDataFrame, row::Symbol, col::Symbol, val::Symbol, fcts::Array{Function,1}; rmna=true)
    if rmna
        ddf = dropna(df, val)
    else
        ddf = df
    end
    pvt = pivot(ddf, row, col, val, fcts[1], rmna=false)
    for v in 2:length(fcts)
        p = pivot(ddf, row, col, val, fcts[v], rmna=false)

        pvt = join(pvt, p , on=row)
    end
    ##println(names(pvt))
    colsymbs = sort(levels(ddf[col]))
    nam = [row]
    for vv in fcts, cc in sort(levels(ddf[col]))
        nam = vcat(nam, symbol("$(string(col))_$(cc)_$(val)_$(vv)"))
    end
    ##println(nam)
    names!(pvt, nam)
    pvt
end

function stats(df::AbstractDataFrame, col::Symbol, fcts = [mean, std, minimum, maximum])
    res = DataFrame()
    n = size(df,1)
    if n==0; return DataFrame(); end
    v = dropna(df[col])
    for f in fcts
        res = hcat(res, [f(v)])
    end

    res = hcat(res, [(n-length(v))/n])
    names!(res, vcat([symbol("$(col)_$(string(fcts[j]))") for j in 1:length(fcts)], symbol("$(col)_NA")))
end

function stats(v::Array, fcts = [mean, std, minimum, maximum])
    res = DataFrame()
   ## n = length(da)

    ##v = dropna(da)
    if length(v)==0; return DataFrame(); end
    for f in fcts
        res = hcat(res, [f(v)])
    end

    names!(res, [symbol("$(string(fcts[j]))") for j in 1:length(fcts)])
    res
end

### http://rosettacode.org/wiki/Sparkline_in_unicode#Julia
function sparklineit(a)
    const sparkchars = '\u2581':'\u2588'
    const dyn = length(sparkchars)
    (lo, hi) = extrema(a)
    b = max(iceil(dyn*(a-lo)/(hi-lo)), 1)
    return join(sparkchars[b], "")
end




function column_description(colcol)
    col = dropna(colcol)
    try
        if eltype(col)<:Number
            e, h = hist(dropnan(col))
            spl = sparklineit(h)
            @sprintf("%.1f %s %.1f", e[1], spl, e[end])
        elseif eltype(col)<:TimeType
            Tmin, Tmax = extrema(col)
            "$Tmin ⋯ $Tmax"
        else
            ll = levels(col)
            if length(ll)<7
                join(["`$(ll[i])`" for i in 1:length(ll)], ", ", " & ")
            else
                join(["`$(ll[i])`" for i in 1:6], ", ")*", …"
            end
        end
    catch exc
        "$exc"
    end
end


import StatsBase.summarystats
function summarystats(df::AbstractDataFrame)
    Nrow = nrow(df)
    nam = names(df)
    percNA = 100*(countna(df) ./ Nrow)
    percNAN = 100*(countnan(df) ./ Nrow)

    descript = [column_description(df[n]) for n in nam]
    DataFrame(field=nam, eltype=eltypes(df), variety=[length(levels(df[n])) for n in nam], PercNA=percNA, PercNotANum=percNAN, Description=descript)
end

function human_numeric(vec::DataArray{UTF8String,1})
    v = DataArray(Float64, 0)
    for k in 1:length(vec)
        if isna(vec[k])
            push!(v, NA)
            continue
        end
        m = match(r"^([+\-0-9\.]+)%", vec[k])
        if !isa(m, Nothing)
            push!(v, float(m.captures[1])/100.0)
            continue
        end
        m = match(r"^([+\-0-9\.]+)\w", vec[k])
        if !isa(m, Nothing)
            push!(v, float(m.captures[1]))
            continue
        end
        push!(v, NA)
    end
    v
end



###
### NA Not Available
###

countna(::Array) = 0
function countna(a::DataArray)
    countnz(isna(a))
end

function countna(df::DataFrame)
    int([countna(df[k]) for k in 1:ncol(df)])
end


###
### NaN Not a Number
###

countnan(::Array) = 0
function countnan{T<:Number}(a::Array{T,1})
    countnz(isnan(a))
end

function countnan(a::DataArray)
    countnan(dropna(a))
end

function countnan(df::DataFrame)
    int([countnan(df[k]) for k in 1:ncol(df)])
end


dropnan(a::Array) = a
function dropnan{T<:Number}(a::Array{T,1})
    a[!isnan(a)]
end

import DataArrays.dropna

dropna(df::AbstractDataFrame) = dropna(df, names(df))
dropna(df::AbstractDataFrame, col::Symbol) = dropna(df, [col])
dropna(df::AbstractDataFrame, col::Symbol, cols...) = dropna(df, ( v=vcat([col], cols); v[ [isa(v[i], Symbol) for i in 1:length(v)] ] ) )
function dropna(df::AbstractDataFrame, cols::Array{Symbol,1})
    filter = trues(nrow(df))
    for c in cols
        if eltype(df[c])<:Number
            filter &= !isna(df[c]) & !isnan(df[c])
        else
            filter &= !isna(df[c])
        end
    end #c
    df[filter, :]
end





##function call(::Type{DataFrames.DataFrame}, d::Array{Base.Dict, 1})
function jsondecode(::Type{DataFrames.DataFrame}, d::Array{Any, 1})
    klist =  collect(keys(d[1]))
    ncol = length(klist)
    nrow = length(d)
    df = DataFrame()
    for c in 1:ncol
        col = @data [d[j][klist[c]] for j in 1:nrow]
        df = hcat(df, DataFrame(x=col))
    end
    names!(df, Base.Symbol[symbol(ky) for ky in klist])
    df
end
export jsondecode



@doc """
# Set Element Type
seteltype!(df, column, type)
sets the data-type of the elements in column 'column' to type 'type'
""" ->
function seteltype!(df::DataFrame, col::Symbol, T::DataType)
    df[col] = T[parse(Int64, s) for s in df[col]]
end
seteltype!(T::DataType, df::DataFrame, col::Symbol) = seteltype!(df, col, T)


import JSON.json

function json(df::DataFrame)
    "[ \n$(join([ "{"*join(["\"$n\":\"$(df[k, n])\"" for n in names(df)], ", ")*"}" for k in 1:nrow(df) ], ",\n") )\n]"
end

import StatsBase.describe
describe(t::Type) = describe(STDOUT, t)
function describe(io, t::Type)
    for n in fieldnames(t)
        write(io, "$n\n$(fieldtype(t, n))\n\n")
    end
end

@doc """
    maprow(df, cols, fct[, typ])

maprow applies a function to each row in the dataframe. The specified columns provide the arguments to
the given function. The arity of the function must match the number of columns.
An optional type argument can be provided for the data type of the result column.

`maprow` can also be used in the do...end notation.
""" ->
function maprow(df::AbstractDataFrame, cols::Union{Symbol, Array{Symbol,1}}, fct::Function, T::Type=Any)
    res = DataArray(T, nrow(df))
    for k in 1:nrow(df)
        args = [df[cols][k, j] for j in 1:length(cols)]
        if !reduce(|, map(isna, args))
            res[k] = fct(args...)
        end
    end
    res
end
maprow(fct::Function, df::AbstractDataFrame, cols::Union{Symbol, Array{Symbol,1}}, T::Type=Any) = maprow(df, cols, fct, T)


function lattice(fct::Function, df::AbstractDataFrame, rows::Union{Symbol, Array{Symbol,1}}, cols::Union{Symbol, Array{Symbol,1}}, fargs...;
        rowstart::Function=(args...)->nothing, rowend::Function=(args...)->nothing,
        cellstart::Function=(args...)->nothing, cellend::Function=(args...)->nothing,
        defaultaction::Function=(args...)->nothing
    )

    Nrow = typeof(rows)<:AbstractArray ? reduce(*, [length(levels(df[s])) for s in rows]) : length(levels(df[rows]))
    Ncol = typeof(cols)<:AbstractArray ? reduce(*, [length(levels(df[s])) for s in cols]) : length(levels(df[cols]))
    r = 0
    c = 0
    res = cell(Nrow, Ncol)
    by(dropna(df, rows), rows) do rf
        r += 1
        rowstart(rf, fargs...)
        by(dropna(rf, cols), cols) do cf
            c +=1
            cellstart(cf, fargs...)
            res[r,c] = fct(cf, fargs...)
            cellend(cf, fargs...)
        end
        rowend(rf, fargs...)
        c = 0
    end

    for r in 1:Nrow, c in 1:Ncol
        if !isassigned(res, r, c)
            res[r,c] = defaultaction(fargs...)
        end
    end
    res
end

import DataFrames.unique
@doc """
`unique(df::AbstractDataFrame, cols::AbstractArray{Symbol,1}, fct::Function)`

Combines rows so that columns given by `cols` have unique values. All other values will be aggregated
by the fiven function `fct`. If the function does not produce valid results `NA` will be used.
The methods uses try-catch, hence, it can be slow.
""" ->
function unique(df::AbstractDataFrame, cols::AbstractArray{Symbol,1}, fct::Function)
    flds = setdiff(names(df), cols)
    by(df, cols) do f
        tempdf = DataFrame()
        for s in flds
            try
                tempdf[s] = fct(f[s])
            catch
                tempdf[s] = NA
            end
        end
        tempdf
    end
end

function unique(df::AbstractDataFrame, cols::AbstractArray{Symbol,1}; otherfields...)
    by(df, cols) do f
        tempdf = DataFrame()
        for (s, fct) in otherfields
            try
                tempdf[s] = fct(f[s])
            catch
                tempdf[s] = NA
            end
        end
        tempdf
    end
end

export unique


#=@doc """
A table with columns :XYZ_pred, :XYZ_pred2, ....

`longformat(cltab, [:x_pred, :y_pred, :XYZ_pred, :info], [""; ["$i" for i in 2:5]])`
""" -> =#
function longformat(df::AbstractDataFrame, symbs::Array{Symbol,1},
        labels::Array{ASCIIString,1}, rankcol::Symbol=:rank)
    cols = setdiff(names(df), vec([symbol("$s$l") for s in symbs, l in labels]))

    ndf = DataFrame()
    for j in 1:length(labels)
        symbs2 = Symbol[symbol("$(s)$(labels[j])") for s in symbs]
        if symbs==symbs2
            tmpdf = df[vcat(cols, symbs2)]
        else
            tmpdf = rename(df[vcat(cols, symbs2)], symbs2, symbs)
        end
        tmpdf[rankcol] = j
        ndf = vcat(ndf, tmpdf)
    end
    ndf
end

function wideformat(args...)
    warn("Not yet implemented!!!")
end


@doc """
`matrixtodataframe` converts a matrix to a data frame whereby each column is named by `stem` plus the
respective character of `indexchars`. The rows of the matrix become data rows.
""" ->
function matrixtodataframe{T}(M::Array{T, 2}, stem, indexchars)
    @assert length(indexchars)==size(M,2)
    df = DataFrame()
    for j in 1:size(M, 2)
        df = hcat(df, DataFrame(x=vec(M[:, j])))
    end
    names!(df, [symbol("$(stem)$(indexchars[j])") for j in 1:size(M, 2)])
    df
end

function matrixtodataframe{T}(M::Array{T, 1}, stem, indexchars)
    @assert length(indexchars)==size(M,1)
    df = DataFrame()
    for j in 1:size(M, 1)
        df = hcat(df, DataFrame(x=M[j]))
    end
    names!(df, [symbol("$(stem)$(indexchars[j])") for j in 1:size(M, 1)])
    df
end
