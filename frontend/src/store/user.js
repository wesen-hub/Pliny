import { defineStore } from 'pinia'
import { login as apiLogin, getMe } from '../api'

export const useUserStore = defineStore('user', {
  state: () => ({
    token: localStorage.getItem('dcp_token') || '',
    user: JSON.parse(localStorage.getItem('dcp_user') || 'null')
  }),
  getters: {
    roles: (state) => (state.user && state.user.roles) || [],
    isAdmin: (state) => (state.user && state.user.roles || []).includes('ADMIN'),
    canWrite: (state) => {
      const roles = (state.user && state.user.roles) || []
      return !roles.includes('VIEWER') && !roles.includes('AUDITOR')
    }
  },
  actions: {
    async login(username, password) {
      const data = await apiLogin({ username, password })
      this.token = data.token
      this.user = data.user
      localStorage.setItem('dcp_token', data.token)
      localStorage.setItem('dcp_user', JSON.stringify(data.user))
    },
    async fetchMe() {
      try {
        this.user = await getMe()
        localStorage.setItem('dcp_user', JSON.stringify(this.user))
      } catch (e) { /* 401 由拦截器处理 */ }
    },
    logout() {
      this.token = ''
      this.user = null
      localStorage.removeItem('dcp_token')
      localStorage.removeItem('dcp_user')
    }
  }
})
