#include "Progress.h"

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Core/ProtocolDefines.h>


namespace DB
{
void ProgressValues::read(ReadBuffer & in, UInt64 server_revision)
{
    readVarUInt(read_rows, in);
    readVarUInt(read_compressed_bytes, in);
    readVarUInt(os_read_bytes, in);
    readVarUInt(read_decompressed_blocks, in);
    readVarUInt(read_decompressed_bytes, in);
    readVarUInt(selected_bytes, in);
    readVarUInt(selected_marks, in);
    readVarUInt(selected_rows, in);
    readVarUInt(selected_parts, in);
    readVarUInt(read_bytes, in);
    readVarUInt(total_rows_to_read, in);
    if (server_revision >= DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO)
    {
        readVarUInt(written_rows, in);
        readVarUInt(written_bytes, in);
    }
    if (server_revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS)
    {
        readVarUInt(elapsed_ns, in);
    }
}


void ProgressValues::write(WriteBuffer & out, UInt64 client_revision) const
{
    writeVarUInt(read_rows, out);
    writeVarUInt(read_compressed_bytes, out);
    writeVarUInt(os_read_bytes, out);
    writeVarUInt(read_decompressed_blocks, out);
    writeVarUInt(read_decompressed_bytes, out);
    writeVarUInt(selected_bytes, out);
    writeVarUInt(selected_marks, out);
    writeVarUInt(selected_rows, out);
    writeVarUInt(selected_parts, out);
    writeVarUInt(read_bytes, out);
    writeVarUInt(total_rows_to_read, out);
    if (client_revision >= DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO)
    {
        writeVarUInt(written_rows, out);
        writeVarUInt(written_bytes, out);
    }
    if (client_revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS)
    {
        writeVarUInt(elapsed_ns, out);
    }
}

void ProgressValues::writeJSON(WriteBuffer & out) const
{
    /// Numbers are written in double quotes (as strings) to avoid loss of precision
    ///  of 64-bit integers after interpretation by JavaScript.

    writeCString("{\"read_rows\":\"", out);
    writeText(read_rows, out);
    writeCString("{\"read_compressed_bytes\":\"", out);
    writeText(read_compressed_bytes, out);
    writeCString("{\"os_read_bytes\":\"", out);
    writeText(os_read_bytes, out);
    writeCString("{\"read_decompressed_blocks\":\"", out);
    writeText(read_decompressed_blocks, out);
    writeCString("{\"read_decompressed_bytes\":\"", out);
    writeText(read_decompressed_bytes, out);
    writeCString("{\"selected_bytes\":\"", out);
    writeText(selected_bytes, out);
    writeCString("{\"selected_marks\":\"", out);
    writeText(selected_marks, out);
    writeCString("{\"selected_rows\":\"", out);
    writeText(selected_rows, out);
    writeCString("{\"selected_parts\":\"", out);
    writeText(selected_parts, out);
    writeCString("\",\"read_bytes\":\"", out);
    writeText(read_bytes, out);
    writeCString("\",\"written_rows\":\"", out);
    writeText(written_rows, out);
    writeCString("\",\"written_bytes\":\"", out);
    writeText(written_bytes, out);
    writeCString("\",\"total_rows_to_read\":\"", out);
    writeText(total_rows_to_read, out);
    writeCString("\",\"result_rows\":\"", out);
    writeText(result_rows, out);
    writeCString("\",\"result_bytes\":\"", out);
    writeText(result_bytes, out);
    writeCString("\"}", out);
}

bool Progress::incrementPiecewiseAtomically(const Progress & rhs)
{
    read_rows += rhs.read_rows;
    read_bytes += rhs.read_bytes;
    read_compressed_bytes += rhs.read_compressed_bytes;
    selected_parts += rhs.selected_parts;
    os_read_bytes += rhs.os_read_bytes;
    read_decompressed_blocks += rhs.read_decompressed_blocks;
    read_decompressed_bytes += rhs.read_decompressed_bytes;
    selected_bytes += rhs.selected_bytes;
    selected_marks += rhs.selected_marks;
    selected_rows += rhs.selected_rows;

    total_rows_to_read += rhs.total_rows_to_read;
    total_bytes_to_read += rhs.total_bytes_to_read;

    written_rows += rhs.written_rows;
    written_bytes += rhs.written_bytes;

    result_rows += rhs.result_rows;
    result_bytes += rhs.result_bytes;

    elapsed_ns += rhs.elapsed_ns;

    return rhs.read_rows || rhs.written_rows;
}

void Progress::reset()
{
    read_rows = 0;
    read_compressed_bytes = 0;
    selected_parts = 0;
    os_read_bytes = 0;
    read_decompressed_blocks = 0;
    read_decompressed_bytes = 0;
    selected_bytes = 0;
    selected_marks = 0;
    selected_rows = 0;
    read_bytes = 0;

    total_rows_to_read = 0;
    total_bytes_to_read = 0;

    written_rows = 0;
    written_bytes = 0;

    result_rows = 0;
    result_bytes = 0;

    elapsed_ns = 0;
}

ProgressValues Progress::getValues() const
{
    ProgressValues res;

    res.read_rows = read_rows.load(std::memory_order_relaxed);
    res.read_bytes = read_bytes.load(std::memory_order_relaxed);
    res.read_compressed_bytes = read_compressed_bytes.load(std::memory_order_relaxed);
    res.selected_parts = selected_parts.load(std::memory_order_relaxed);
    res.os_read_bytes = os_read_bytes.load(std::memory_order_relaxed);
    res.read_decompressed_blocks = read_decompressed_blocks.load(std::memory_order_relaxed);
    res.read_decompressed_bytes = read_decompressed_bytes.load(std::memory_order_relaxed);
    res.selected_bytes = selected_bytes.load(std::memory_order_relaxed);
    res.selected_marks = selected_marks.load(std::memory_order_relaxed);
    res.selected_rows = selected_rows.load(std::memory_order_relaxed);

    res.total_rows_to_read = total_rows_to_read.load(std::memory_order_relaxed);
    res.total_bytes_to_read = total_bytes_to_read.load(std::memory_order_relaxed);

    res.written_rows = written_rows.load(std::memory_order_relaxed);
    res.written_bytes = written_bytes.load(std::memory_order_relaxed);

    res.result_rows = result_rows.load(std::memory_order_relaxed);
    res.result_bytes = result_bytes.load(std::memory_order_relaxed);

    res.elapsed_ns = elapsed_ns.load(std::memory_order_relaxed);

    return res;
}

ProgressValues Progress::fetchValuesAndResetPiecewiseAtomically()
{
    ProgressValues res;

    res.read_rows = read_rows.fetch_and(0);
    res.read_bytes = read_bytes.fetch_and(0);
    res.read_compressed_bytes = read_compressed_bytes.fetch_and(0);
    res.selected_parts = selected_parts.fetch_and(0);
    res.os_read_bytes = os_read_bytes.fetch_and(0);
    res.read_decompressed_blocks = read_decompressed_blocks.fetch_and(0);
    res.read_decompressed_bytes = read_decompressed_bytes.fetch_and(0);
    res.selected_bytes = selected_bytes.fetch_and(0);
    res.selected_marks = selected_marks.fetch_and(0);
    res.selected_rows = selected_rows.fetch_and(0);

    res.total_rows_to_read = total_rows_to_read.fetch_and(0);
    res.total_bytes_to_read = total_bytes_to_read.fetch_and(0);

    res.written_rows = written_rows.fetch_and(0);
    res.written_bytes = written_bytes.fetch_and(0);

    res.result_rows = result_rows.fetch_and(0);
    res.result_bytes = result_bytes.fetch_and(0);

    res.elapsed_ns = elapsed_ns.fetch_and(0);

    return res;
}

Progress Progress::fetchAndResetPiecewiseAtomically()
{
    Progress res;

    res.read_rows = read_rows.fetch_and(0);
    res.read_bytes = read_bytes.fetch_and(0);
    res.read_compressed_bytes = read_compressed_bytes.fetch_and(0);
    res.selected_parts = selected_parts.fetch_and(0);
    res.os_read_bytes = os_read_bytes.fetch_and(0);
    res.read_decompressed_blocks = read_decompressed_blocks.fetch_and(0);
    res.read_decompressed_bytes = read_decompressed_bytes.fetch_and(0);
    res.selected_bytes = selected_bytes.fetch_and(0);
    res.selected_marks = selected_marks.fetch_and(0);
    res.selected_rows = selected_rows.fetch_and(0);

    res.total_rows_to_read = total_rows_to_read.fetch_and(0);
    res.total_bytes_to_read = total_bytes_to_read.fetch_and(0);

    res.written_rows = written_rows.fetch_and(0);
    res.written_bytes = written_bytes.fetch_and(0);

    res.result_rows = result_rows.fetch_and(0);
    res.result_bytes = result_bytes.fetch_and(0);

    res.elapsed_ns = elapsed_ns.fetch_and(0);

    return res;
}

Progress & Progress::operator=(Progress && other) noexcept
{
    read_rows = other.read_rows.load(std::memory_order_relaxed);
    read_bytes = other.read_bytes.load(std::memory_order_relaxed);
    read_compressed_bytes = other.read_compressed_bytes.load(std::memory_order_relaxed);
    selected_parts = other.selected_parts.load(std::memory_order_relaxed);
    os_read_bytes = other.os_read_bytes.load(std::memory_order_relaxed);
    read_decompressed_blocks = other.read_decompressed_blocks.load(std::memory_order_relaxed);
    read_decompressed_bytes = other.read_decompressed_bytes.load(std::memory_order_relaxed);
    selected_bytes = other.selected_bytes.load(std::memory_order_relaxed);
    selected_marks = other.selected_marks.load(std::memory_order_relaxed);
    selected_rows = other.selected_rows.load(std::memory_order_relaxed);

    total_rows_to_read = other.total_rows_to_read.load(std::memory_order_relaxed);
    total_bytes_to_read = other.total_bytes_to_read.load(std::memory_order_relaxed);

    written_rows = other.written_rows.load(std::memory_order_relaxed);
    written_bytes = other.written_bytes.load(std::memory_order_relaxed);

    result_rows = other.result_rows.load(std::memory_order_relaxed);
    result_bytes = other.result_bytes.load(std::memory_order_relaxed);

    elapsed_ns = other.elapsed_ns.load(std::memory_order_relaxed);

    return *this;
}

void Progress::read(ReadBuffer & in, UInt64 server_revision)
{
    ProgressValues values;
    values.read(in, server_revision);

    read_rows.store(values.read_rows, std::memory_order_relaxed);
    read_bytes.store(values.read_bytes, std::memory_order_relaxed);
    read_compressed_bytes.store(values.read_compressed_bytes, std::memory_order_relaxed);
    selected_parts.store(values.selected_parts, std::memory_order_relaxed);
    os_read_bytes.store(values.os_read_bytes, std::memory_order_relaxed);
    read_decompressed_blocks.store(values.read_decompressed_blocks, std::memory_order_relaxed);
    read_decompressed_bytes.store(values.read_decompressed_bytes, std::memory_order_relaxed);
    selected_bytes.store(values.selected_bytes, std::memory_order_relaxed);
    selected_marks.store(values.selected_marks, std::memory_order_relaxed);
    selected_rows.store(values.selected_rows, std::memory_order_relaxed);
    total_rows_to_read.store(values.total_rows_to_read, std::memory_order_relaxed);

    written_rows.store(values.written_rows, std::memory_order_relaxed);
    written_bytes.store(values.written_bytes, std::memory_order_relaxed);

    elapsed_ns.store(values.elapsed_ns, std::memory_order_relaxed);
}

void Progress::write(WriteBuffer & out, UInt64 client_revision) const
{
    getValues().write(out, client_revision);
}

void Progress::writeJSON(WriteBuffer & out) const
{
    getValues().writeJSON(out);
}

}
