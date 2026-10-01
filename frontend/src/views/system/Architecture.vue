<template>
  <div class="arch-page">
    <iframe
      src="/arch/index.html"
      class="arch-frame"
      title="系统架构设计图"
    />
  </div>
</template>

<script setup>
// 系统架构设计图:交互式架构图(可下钻),内容见 public/arch/index.html
</script>

<style scoped>
.arch-page {
  height: calc(100vh - 60px);
  padding: 0;
  background: #eef2f8;
}
.arch-frame {
  width: 100%;
  height: 100%;
  border: none;
  display: block;
}
</style>
