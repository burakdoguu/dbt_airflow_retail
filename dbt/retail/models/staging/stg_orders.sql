with source as (

    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}
    select * from {{ ref('orders') }}

),

renamed as (

    select
        event,
        messageid,
        userid,
        quantity,
        orderid,
        productid

    from source

)

select * from renamed
